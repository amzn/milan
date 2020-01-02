package com.amazon.milan.flink.application.sinks

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, ZoneOffset}
import java.util.{Timer, TimerTask, UUID}

import com.amazon.milan.flink.application.FlinkDataSink
import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.storage.aws.{RegionS3ClientFactory, S3ClientFactory, S3Util}
import com.amazon.milan.typeutil.TypeDescriptor
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model._

import scala.collection.JavaConverters._


class FlinkS3DataSink[T](region: String, prefix: String, keyFormat: String) extends FlinkDataSink[T] {
  override def getSinkFunction: SinkFunction[_] = {
    val s3ClientFactory = new RegionS3ClientFactory(this.region)
    val (bucket, key) = S3Util.parseS3Uri(this.prefix)
    new S3SinkFunction[T](s3ClientFactory, bucket, key, this.keyFormat)
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

  @transient private var currentUpload: CreateMultipartUploadResponse = _
  @transient private var currentUploadSize: Int = 0
  @transient private var nextPartNumber: Int = 1
  @transient private var currentPartEtags: List[String] = _
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

      val body = RequestBody.fromBytes(bytes)

      val request = UploadPartRequest.builder()
        .bucket(this.currentUpload.bucket())
        .key(this.currentUpload.key())
        .uploadId(this.currentUpload.uploadId())
        .partNumber(this.nextPartNumber)
        .contentLength(bytes.length.toLong)
        .build()

      val result = this.s3Client.uploadPart(request, body)
      this.currentPartEtags = this.currentPartEtags :+ result.eTag()
      this.nextPartNumber = this.nextPartNumber + 1
      this.currentUploadSize = this.currentUploadSize + bytes.length
      this.lastWriteTime = Instant.now()

      this.logger.info(s"Multi-part upload to 's3://${this.currentUpload.bucket()}/${this.currentUpload.key()}' uploaded part ${this.nextPartNumber - 1}, current upload size is ${this.currentUploadSize} bytes.")

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
      this.logger.info(s"Completing multi-part upload to 's3://${upload.bucket()}/${upload.key()}'. Part ETags: [${this.currentPartEtags.mkString(", ")}].")

      val completedParts = this.currentPartEtags.zipWithIndex.map {
        case (etag, index) => CompletedPart.builder().eTag(etag).partNumber(index).build()
      }

      val completedUpload = CompletedMultipartUpload.builder()
        .parts(completedParts.asJavaCollection)
        .build()

      val request = CompleteMultipartUploadRequest.builder()
        .bucket(upload.bucket())
        .key(upload.key())
        .uploadId(upload.uploadId())
        .multipartUpload(completedUpload)
        .build()

      this.s3Client.completeMultipartUpload(request)
    }
  }

  private def startNewUpload(): Unit = {
    val request = CreateMultipartUploadRequest.builder()
      .bucket(this.bucket)
      .key(this.createDestinationKey())
      .build()

    this.logger.info(s"Starting multi-part upload to 's3://${request.bucket()}/${request.key()}'.")

    this.currentUploadSize = 0
    this.nextPartNumber = 1
    this.currentUpload = this.s3Client.createMultipartUpload(request)
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
