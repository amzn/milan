package com.amazon.milan.storage

import java.io.{ByteArrayInputStream, InputStream, OutputStream}

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.{GetObjectRequest, ObjectMetadata}
import com.typesafe.scalalogging.Logger
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory


object S3BlobStore {
  /**
   * Creates an [[S3BlobStore]] for the specified region, bucket, and root folder.
   *
   * @param region     The AWS region.
   * @param bucketName The name of an S3 bucket.
   * @param rootFolder The root folder for S3 objects.
   */
  def create(region: Regions, bucketName: String, rootFolder: String): S3BlobStore = {
    val s3ClientFactory = new RegionS3ClientFactory(region)
    new S3BlobStore(s3ClientFactory, bucketName, rootFolder)
  }

  /**
   * Creates an [[S3BlobStore]] for the specified region, bucket, and root folder.
   *
   * @param region     The name of an AWS region.
   * @param bucketName The name of an S3 bucket.
   * @param rootFolder The root folder for S3 objects.
   */
  def create(region: String, bucketName: String, rootFolder: String): S3BlobStore = {
    val s3ClientFactory = new RegionS3ClientFactory(region)
    new S3BlobStore(s3ClientFactory, bucketName, rootFolder)
  }
}


/**
 * A [[BlobStore]] implementation that stores blobs in an S3 bucket.
 *
 * @param s3ClientFactory Factory interface for creating S3 clients.
 * @param bucketName      The name of the S3 bucket.
 * @param rootFolder      The folder prefix to use for objects.
 */
class S3BlobStore(s3ClientFactory: S3ClientFactory, bucketName: String, rootFolder: String) extends BlobStore {
  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))
  @transient private lazy val s3Client = this.s3ClientFactory.createClient()

  override def readBlob(key: String, targetStream: OutputStream): Unit = {
    val fullKey = this.getFullKey(key)
    this.logger.info(s"Reading blob from S3 location 's3://${this.bucketName}/$fullKey'.")
    val request = new GetObjectRequest(this.bucketName, fullKey)
    val s3Object = this.s3Client.getObject(request)
    IOUtils.copy(s3Object.getObjectContent, targetStream)
  }

  override def writeBlob(key: String, blobStream: InputStream): Unit = {
    val bytes = IO.readAllBytes(blobStream)

    val metadata = new ObjectMetadata()
    metadata.setContentLength(bytes.length)
    metadata.setContentType("application/octet-stream")

    val fullKey = this.getFullKey(key)
    this.logger.info(s"Copying blob to S3 location 's3://${this.bucketName}/$fullKey'.")
    val result = this.s3Client.putObject(this.bucketName, fullKey, new ByteArrayInputStream(bytes), metadata)

    this.logger.info(s"Blob copy complete (ETag = ${result.getETag}).")

  }

  private def getFullKey(key: String): String = {
    this.rootFolder + "/" + key
  }
}
