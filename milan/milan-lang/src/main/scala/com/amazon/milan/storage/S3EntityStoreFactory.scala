package com.amazon.milan.storage

import com.fasterxml.jackson.databind.ObjectMapper

import scala.reflect.ClassTag


/**
 * [[EntityStoreFactory]] interface for creating S3 entity stores.
 *
 * @param s3ClientFactory Factory interface for creating S3 clients.
 * @param bucketName      The name of the S3 bucket.
 * @param rootFolder      The folder prefix to use for objects.
 * @param objectSuffix    The suffix to append to all object keys.
 * @param objectMapper    The [[ObjectMapper]] used for serialization.
 */
class S3EntityStoreFactory(s3ClientFactory: S3ClientFactory,
                           bucketName: String,
                           rootFolder: String,
                           objectSuffix: String,
                           objectMapper: ObjectMapper)
  extends EntityStoreFactory {

  override def createEntityStore[T: ClassTag](name: String): EntityStore[T] = {
    val storeRootFolder = this.rootFolder + "/" + name
    new S3EntityStore[T](this.s3ClientFactory, this.bucketName, storeRootFolder, this.objectSuffix, this.objectMapper)
  }
}
