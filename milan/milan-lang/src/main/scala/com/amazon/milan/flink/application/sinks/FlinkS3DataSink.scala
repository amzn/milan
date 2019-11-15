package com.amazon.milan.flink.application.sinks

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, ZoneOffset}
import java.util
import java.util.{Timer, TimerTask, UUID}

import com.amazon.milan.flink.application.FlinkDataSink
import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.storage.{RegionS3ClientFactory, S3ClientFactory}
import com.amazon.milan.typeutil.TypeDescriptor
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model._
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


class FlinkS3DataSink[T](region: String, prefix: String, keyFormat: String) extends FlinkDataSink[T] {
  override def getSinkFunction: SinkFunction[_] = {
    val prefixUri = new AmazonS3URI(this.prefix)
    val s3ClientFactory = new RegionS3ClientFactory(this.region)
    new S3SinkFunction[T](s3ClientFactory, prefixUri.getBucket, prefixUri.getKey, this.keyFormat)
  }

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
  }
}


class S3SinkFunction[T](s3ClientFactory: S3ClientFactory,
                        bucket: String,
                        keyPrefix: String,
                        keyFormat: String,
                        chunkTimeoutSeconds: Int = 60,
                        maxChunkSizeBytes: Int = 5242880) extends SinkFunction[T] {

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  @transient private lazy val mapper = new ScalaObjectMapper()
  @transient private lazy val s3Client = this.s3ClientFactory.createClient()
  @transient private lazy val dateTimeFormatter = DateTimeFormatter.ofPattern(this.keyFormat)

  @transient private var currentUpload: InitiateMultipartUploadResult = _
  @transient private var currentUploadSize: Int = 0
  @transient private var nextPartNumber: Int = 1
  @transient private var currentPartEtags: List[PartETag] = _
  @transient private var lastWriteTime: Instant = _

  @transient private var timer: Timer = _

  override def invoke(value: T, context: SinkFunction.Context[_]): Unit = {
    this.synchronized {
      if (this.currentUpload == null) {
        this.startNewUpload()
      }

      val outStream = new ByteArrayOutputStream()
      this.mapper.writeValue(outStream, value)

      // Write a linefeed between records.
      outStream.write("\n".getBytes(StandardCharsets.UTF_8))

      val bytes = outStream.toByteArray

      val partStream = new ByteArrayInputStream(bytes)

      val request = new UploadPartRequest()
        .withBucketName(this.currentUpload.getBucketName)
        .withKey(this.currentUpload.getKey)
        .withUploadId(this.currentUpload.getUploadId)
        .withPartNumber(this.nextPartNumber)
        .withPartSize(bytes.length)
        .withInputStream(partStream)

      val result = this.s3Client.uploadPart(request)
      this.currentPartEtags = this.currentPartEtags :+ result.getPartETag
      this.nextPartNumber = this.nextPartNumber + 1
      this.currentUploadSize = this.currentUploadSize + bytes.length
      this.lastWriteTime = Instant.now()

      this.logger.info(s"Multi-part upload to 's3://${this.currentUpload.getBucketName}/${this.currentUpload.getKey}' uploaded part ${this.nextPartNumber - 1}, current upload size is ${this.currentUploadSize} bytes.")

      if (this.currentUploadSize > this.maxChunkSizeBytes) {
        this.completeUpload()
      }

      this.startTimer()
    }
  }

  private def completeUpload(): Unit = {
    val upload = this.currentUpload
    this.currentUpload = null
    if (upload != null) {
      this.logger.info(s"Completing multi-part upload to 's3://${upload.getBucketName}/${upload.getKey}'. Part ETags: [${this.currentPartEtags.map(_.getETag).mkString(", ")}].")

      val partEtags = new util.ArrayList[PartETag](this.currentPartEtags.asJavaCollection)
      val request = new CompleteMultipartUploadRequest(upload.getBucketName, upload.getKey, upload.getUploadId, partEtags)
      this.s3Client.completeMultipartUpload(request)
    }
  }

  private def startNewUpload(): Unit = {
    val request = new InitiateMultipartUploadRequest(this.bucket, this.createDestinationKey())

    this.logger.info(s"Starting multi-part upload to 's3://${request.getBucketName}/${request.getKey}'.")

    this.currentUploadSize = 0
    this.nextPartNumber = 1
    this.currentUpload = this.s3Client.initiateMultipartUpload(request)
    this.currentPartEtags = List()
  }

  private def createDestinationKey(): String = {
    val formattedKey = this.dateTimeFormatter.format(Instant.now().atOffset(ZoneOffset.UTC))
    s"${this.keyPrefix}$formattedKey-${UUID.randomUUID()}"
  }

  private def startTimer(): Unit = {
    if (this.timer == null) {
      this.timer = new Timer(true)

      val task = new TimerTask {
        override def run(): Unit = checkChunkTimeout()
      }

      // Check for timeout once per second.
      this.timer.scheduleAtFixedRate(task, 1000, 1000)
    }
  }

  private def checkChunkTimeout(): Unit = {
    this.synchronized {
      val secondsSinceLastWrite = Duration.between(this.lastWriteTime, Instant.now()).getSeconds
      if (secondsSinceLastWrite > this.chunkTimeoutSeconds) {
        this.completeUpload()
      }
    }
  }

  override def finalize(): Unit = {
    this.completeUpload()

    val thisTimer = this.timer
    this.timer = null
    if (thisTimer != null) {
      this.logger.info("Cancelling timer.")
      thisTimer.cancel()
    }
  }
}
