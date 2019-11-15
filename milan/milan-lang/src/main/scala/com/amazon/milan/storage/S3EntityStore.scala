package com.amazon.milan.storage

import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.stream.unfold
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.HttpStatus

import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}


/**
 * [[EntityStore]] implementation that stores entities in a folder in an S3 bucket.
 *
 * @param s3ClientFactory Factory interface for creating S3 clients.
 * @param bucketName      The name of the S3 bucket.
 * @param rootFolder      The folder prefix to use for objects.
 * @param objectSuffix    The suffix to append to all object keys.
 * @param objectMapper    The [[ObjectMapper]] used for serialization.
 * @tparam T The type of entities.
 */
class S3EntityStore[T: ClassTag](s3ClientFactory: S3ClientFactory,
                                 bucketName: String,
                                 rootFolder: String,
                                 objectSuffix: String,
                                 objectMapper: ObjectMapper)
  extends EntityStore[T] {

  @transient private lazy val s3Client = this.s3ClientFactory.createClient()

  def this(s3ClientFactory: S3ClientFactory, bucket: String, rootFolder: String, objectSuffix: String) {
    this(s3ClientFactory, bucket, rootFolder, objectSuffix, new ScalaObjectMapper())
  }

  override def entityExists(key: String): Boolean = {
    this.s3Client.doesObjectExist(this.bucketName, this.getFullKey(key))
  }

  override def getEntity(key: String): T = {
    val entityJson =
      try {
        this.s3Client.getObjectAsString(this.bucketName, this.getFullKey(key))
      }
      catch {
        case ex: AmazonS3Exception if ex.getStatusCode == HttpStatus.SC_NOT_FOUND =>
          throw new EntityNotFoundException(key, ex)
      }

    this.objectMapper.readValue(entityJson, classTag[T].runtimeClass.asInstanceOf[Class[T]])
  }

  override def listEntities(): TraversableOnce[T] = {
    listEntityKeys().map(this.getEntity)
  }

  override def listEntityKeys(): TraversableOnce[String] = {
    this.listAllObjects(this.rootFolder).map(this.getShortKey)
  }

  override def putEntity(key: String, value: T): Unit = {
    val entityJson = this.objectMapper.writeValueAsString(value)
    this.s3Client.putObject(this.bucketName, this.getFullKey(key), entityJson)
  }

  private def getFullKey(key: String): String = {
    this.rootFolder + "/" + key + this.objectSuffix
  }

  private def getShortKey(fullKey: String): String = {
    fullKey.substring(this.rootFolder.length + 1, fullKey.length - this.objectSuffix.length)
  }

  private def listAllObjects(folder: String): Array[String] = {
    val listing = this.s3Client.listObjects(this.bucketName, folder)

    val listings =
      Seq(listing) ++
        unfold(listing) {
          previousListing =>
            if (previousListing.isTruncated) {
              val nextListing = this.s3Client.listNextBatchOfObjects(previousListing)
              Some(nextListing, nextListing)
            }
            else {
              None
            }
        }

    listings.flatMap(_.getObjectSummaries.asScala).map(_.getKey).toArray
  }
}
