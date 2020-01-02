package com.amazon.milan.storage.aws

import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.storage.{EntityNotFoundException, EntityStore}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.HttpStatus
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, PutObjectRequest, S3Exception}

import scala.reflect.{ClassTag, classTag}


object S3EntityStore {
  private def getBucket(s3Url: String): String = {
    val (bucket, _) = S3Util.parseS3Uri(s3Url)
    bucket
  }

  private def getRootFolder(s3Url: String): String = {
    val (_, key) = S3Util.parseS3Uri(s3Url)
    key
  }
}


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

  def this(s3ClientFactory: S3ClientFactory, s3Url: String, objectSuffix: String) {
    this(s3ClientFactory, S3EntityStore.getBucket(s3Url), S3EntityStore.getRootFolder(s3Url), objectSuffix)
  }

  override def entityExists(key: String): Boolean = {
    try {
      val request = GetObjectRequest.builder().bucket(this.bucketName).key(this.getFullKey(key)).build()
      this.s3Client.getObject(request)
      true
    }
    catch {
      case ex: S3Exception if ex.statusCode() == HttpStatus.SC_NOT_FOUND =>
        false
    }
  }

  override def getEntity(key: String): T = {
    val entityBytes =
      try {
        val request = GetObjectRequest.builder().bucket(this.bucketName).key(this.getFullKey(key)).build()
        this.s3Client.getObjectAsBytes(request).asByteArray()
      }
      catch {
        case ex: S3Exception if ex.statusCode() == HttpStatus.SC_NOT_FOUND =>
          throw new EntityNotFoundException(key, ex)
      }

    this.objectMapper.readValue(entityBytes, classTag[T].runtimeClass.asInstanceOf[Class[T]])
  }

  override def listEntities(): TraversableOnce[T] = {
    listEntityKeys().map(this.getEntity)
  }

  override def listEntityKeys(): TraversableOnce[String] = {
    this.s3Client.listAllObjects(this.bucketName, this.rootFolder).map(obj => this.getShortKey(obj.key()))
  }

  override def putEntity(key: String, value: T): Unit = {
    val entityBytes = this.objectMapper.writeValueAsBytes(value)
    val request = PutObjectRequest.builder().bucket(this.bucketName).key(this.getFullKey(key)).build()
    val body = RequestBody.fromBytes(entityBytes)
    this.s3Client.putObject(request, body)
  }

  private def getFullKey(key: String): String = {
    this.rootFolder + "/" + key + this.objectSuffix
  }

  private def getShortKey(fullKey: String): String = {
    fullKey.substring(this.rootFolder.length + 1, fullKey.length - this.objectSuffix.length)
  }
}
