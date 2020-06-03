package com.amazon.milan.storage.aws

import java.io.ByteArrayInputStream

import com.amazon.milan.dataformats.DataInputFormat
import com.amazon.milan.storage.IO
import com.amazonaws.services.s3.AmazonS3URI
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest

object S3Util {
  private val logger = Logger(LoggerFactory.getLogger(getClass))

  /**
   * Extracts the bucket and key from an S3 URI.
   *
   * @param uri An s3 uri.
   * @return A tuple of the bucket and key extracted from the URI.
   */
  def parseS3Uri(uri: String): (String, String) = {
    val s3Uri = new AmazonS3URI(uri)
    (s3Uri.getBucket, s3Uri.getKey)
  }

  /**
   * Reads records from all objects with a prefix in a bucket.
   *
   * @param s3Client   An S3 client.
   * @param bucket     An S3 bucket.
   * @param prefix     The prefix of objects to read.
   * @param dataFormat The data format of the records in the objects.
   * @tparam T The type of records.
   * @return A [[TraversableOnce]] that yields the records read from the objects in S3.
   */
  def readAllRecords[T](s3Client: S3Client,
                        bucket: String,
                        prefix: String,
                        dataFormat: DataInputFormat[T]): TraversableOnce[T] = {
    this.logger.info(s"Reading records from objects in bucket '$bucket' with prefix '$prefix'.")

    s3Client.listAllObjects(bucket, prefix)
      .flatMap(obj => this.readS3File(s3Client, bucket, obj.key(), dataFormat))
  }

  /**
   * Reads all records from an object in S3.
   *
   * @param s3Client   An S3 client.
   * @param bucket     An S3 bucket.
   * @param key        The key of the object.
   * @param dataFormat The data format to use for reading records.
   * @tparam T The type of records.
   * @return A [[TraversableOnce]] that yields the records read from the S3 object.
   */
  private def readS3File[T](s3Client: S3Client,
                            bucket: String,
                            key: String,
                            dataFormat: DataInputFormat[T]): TraversableOnce[T] = {
    this.logger.info(s"Reading records in file '$bucket/$key'.")

    val obj = s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())
    val content = IO.readAllBytes(obj)
    val contentStream = new ByteArrayInputStream(content)

    dataFormat.readValues(contentStream)
  }
}
