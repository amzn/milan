package com.amazon.milan.experimentation

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.regex.Pattern

import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.storage.IO
import com.amazon.milan.storage.aws._
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, PutObjectRequest}

import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}


object Datasets {
  private val mapper = new ScalaObjectMapper()
  private val logger = Logger(LoggerFactory.getLogger(getClass))
  private val timestampPattern = this.getTimestampPattern

  def writeDataset[T](items: Seq[T], path: String): Unit = {
    if (path.startsWith("s3://")) {
      val (bucket, key) = S3Util.parseS3Uri(path)
      this.writeToS3(items, bucket, key)
    }
    else {
      this.writeToFile(items, Paths.get(path))
    }
  }

  def writeToS3[T](items: Seq[T], bucket: String, key: String): Unit = {
    val tempFilePath = Files.createTempFile("experiment_data", ".tmp")
    this.logger.info(s"Writing to temporary file '$tempFilePath'.")

    try {
      this.writeToFile(items, tempFilePath)

      val s3 = S3Client.builder().build()
      s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), tempFilePath)
    }
    finally {
      this.logger.info(s"Deleting temporary file '$tempFilePath'.")
      Files.delete(tempFilePath)
    }
  }

  def writeToFile[T](items: Seq[T], path: Path): Unit = {
    items.foreach(r => Files.write(path, (this.mapper.writeValueAsString(r) + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND))
  }

  def readFiles[T: ClassTag](path: String): TraversableOnce[T] = {
    if (path.startsWith("s3://")) {
      val (bucket, key) = S3Util.parseS3Uri(path)
      this.readS3Files(bucket, key)
    }
    else {
      throw new IllegalArgumentException(s"Unsupported path: '$path'.")
    }
  }

  def readS3Files[T: ClassTag](bucket: String, key: String, startDate: Instant, endDate: Instant): TraversableOnce[T] = {
    val s3Client = S3Client.builder().build()
    this.readS3Files[T](s3Client, bucket, key, startDate, endDate)
  }

  def readS3Files[T: ClassTag](bucket: String, key: String): TraversableOnce[T] =
    this.readS3Files(S3Client.builder().build(), bucket, key)

  def readS3Files[T: ClassTag](s3Client: S3Client, bucket: String, key: String): TraversableOnce[T] =
    this.readS3Files[T](s3Client, bucket, key, (_: String) => true)

  def readS3Files[T: ClassTag](s3Client: S3Client, bucket: String, key: String, startDate: Instant, endDate: Instant): TraversableOnce[T] = {
    val filter = (key: String) => isDateBetween(key, startDate, endDate)
    this.readS3Files[T](s3Client, bucket, key, filter)
  }

  def readS3Files[T: ClassTag](s3Client: S3Client, bucket: String, key: String, objectKeyFilter: String => Boolean): TraversableOnce[T] = {
    val objectKeys = this.getAllObjectKeys(s3Client, bucket, key)
    val filteredKeys = objectKeys.filter(objectKeyFilter)

    filteredKeys.flatMap(key => this.readS3File[T](s3Client, bucket, key))
  }

  private def readS3File[T: ClassTag](s3Client: S3Client, bucket: String, key: String): Seq[T] = {
    this.logger.info(s"Reading records in file '$bucket/$key'.")

    val valueType = classTag[T].runtimeClass.asInstanceOf[Class[T]]

    val obj = s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())
    val content = IO.readAllBytes(obj)
    val contentStream = new ByteArrayInputStream(content)

    this.mapper.readerFor(valueType).readValues[T](contentStream).asScala.toSeq
  }

  private def getAllObjectKeys(s3Client: S3Client, bucket: String, prefix: String): TraversableOnce[String] = {
    s3Client.listAllObjects(bucket, prefix).map(_.key())
  }

  private def getTimestampPattern: Pattern = {
    // Define patterns for different components in a timestamp: yyyy-MM-dd-HH-mm-ss
    val twoDigits = "\\d{2}"
    val fourDigits = "\\d{4}"
    Pattern.compile(s".*($fourDigits)-($twoDigits)-($twoDigits)-($twoDigits)-($twoDigits)-($twoDigits).*")
  }

  private def isDateBetween(objectKey: String, startDate: Instant, endDate: Instant): Boolean = {
    val matcher = this.timestampPattern.matcher(objectKey)
    if (!matcher.matches()) {
      false
    }
    else {
      val year = matcher.group(1).toInt
      val month = matcher.group(2).toInt
      val day = matcher.group(3).toInt
      val hour = matcher.group(4).toInt
      val minute = matcher.group(5).toInt
      val second = matcher.group(6).toInt
      val epochSeconds = LocalDateTime.of(year, month, day, hour, minute, second).toEpochSecond(ZoneOffset.UTC)
      val timeStamp = Instant.ofEpochSecond(epochSeconds)
      (startDate.equals(timeStamp) || startDate.isBefore(timeStamp)) && timeStamp.isBefore(endDate)
    }
  }
}
