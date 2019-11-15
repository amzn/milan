package com.amazon.milan.experimentation

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.regex.Pattern

import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.storage.IO
import com.amazon.milan.stream.unfold
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, AmazonS3URI}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}


object Datasets {
  private val mapper = new ScalaObjectMapper()
  private val logger = Logger(LoggerFactory.getLogger(getClass))
  private val timestampPattern = this.getTimestampPattern

  def writeDataset[T](items: Seq[T], path: String): Unit = {
    if (path.startsWith("s3://")) {
      this.writeToS3(items, path)
    }
    else {
      this.writeToFile(items, Paths.get(path))
    }
  }

  def writeToS3[T](items: Seq[T], uri: String): Unit = {
    val s3Uri = new AmazonS3URI(uri)

    val tempFilePath = Files.createTempFile("experiment_data", ".tmp")
    this.logger.info(s"Writing to temporary file '$tempFilePath'.")

    try {
      this.writeToFile(items, tempFilePath)

      val s3 = AmazonS3ClientBuilder.defaultClient()
      s3.putObject(s3Uri.getBucket, s3Uri.getKey, tempFilePath.toFile)
    }
    finally {
      this.logger.info(s"Deleting temporary file '$tempFilePath'.")
      Files.delete(tempFilePath)
    }
  }

  def writeToFile[T](items: Seq[T], path: Path): Unit = {
    items.foreach(r => Files.write(path, (this.mapper.writeValueAsString(r) + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND))
  }

  def readFiles[T: ClassTag](path: String): Seq[T] = {
    if (path.startsWith("s3://")) {
      this.readS3Files(path)
    }
    else {
      throw new IllegalArgumentException(s"Unsupported path: '$path'.")
    }
  }

  def readS3Files[T: ClassTag](s3Uri: String, startDate: Instant, endDate: Instant): Seq[T] = {
    val s3Client = AmazonS3ClientBuilder.defaultClient()
    this.readS3Files[T](s3Client, s3Uri, startDate, endDate)
  }

  def readS3Files[T: ClassTag](s3Uri: String): Seq[T] =
    this.readS3Files(AmazonS3ClientBuilder.defaultClient(), s3Uri)

  def readS3Files[T: ClassTag](s3Client: AmazonS3, s3Uri: String): Seq[T] =
    this.readS3Files[T](s3Client, s3Uri, (_: String) => true)

  def readS3Files[T: ClassTag](s3Client: AmazonS3, s3Uri: String, startDate: Instant, endDate: Instant): Seq[T] = {
    val filter = (key: String) => isDateBetween(key, startDate, endDate)
    this.readS3Files[T](s3Client, s3Uri, filter)
  }

  def readS3Files[T: ClassTag](s3Client: AmazonS3, s3Uri: String, objectKeyFilter: String => Boolean): Seq[T] = {
    val uri = new AmazonS3URI(s3Uri)
    val objectKeys = this.getAllObjectKeys(s3Client, uri.getBucket, uri.getKey)
    val filteredKeys = objectKeys.filter(objectKeyFilter)

    filteredKeys.flatMap(key => this.readS3File[T](s3Client, uri.getBucket, key))
  }

  private def readS3File[T: ClassTag](s3Client: AmazonS3, bucket: String, key: String): Seq[T] = {
    this.logger.info(s"Reading records in file '$bucket/$key'.")

    val valueType = classTag[T].runtimeClass.asInstanceOf[Class[T]]

    val obj = s3Client.getObject(bucket, key)
    val content = IO.readAllBytes(obj.getObjectContent)
    val contentStream = new ByteArrayInputStream(content)

    this.mapper.readerFor(valueType).readValues[T](contentStream).asScala.toSeq
  }

  private def getAllObjectKeys(s3Client: AmazonS3, bucket: String, prefix: String): Seq[String] = {
    val listing = s3Client.listObjects(bucket, prefix)

    val listings =
      Seq(listing) ++
        unfold(listing) {
          previousListing =>
            if (previousListing.isTruncated) {
              val nextListing = s3Client.listNextBatchOfObjects(previousListing)
              Some(nextListing, nextListing)
            }
            else {
              None
            }
        }

    listings.flatMap(_.getObjectSummaries.asScala).map(_.getKey)
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
