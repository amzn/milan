package com.amazon.milan.storage


/**
 * An implementation of [[BlobStoreFactory]] that creates S3 blob stores.
 *
 * @param s3ClientFactory Factory interface for creating S3 clients.
 * @param bucketName      The name of the S3 bucket.
 * @param rootFolder      The folder prefix to use for stores.
 */
class S3BlobStoreFactory(s3ClientFactory: S3ClientFactory, bucketName: String, rootFolder: String)
  extends BlobStoreFactory {

  override def createBlobStore(name: String): BlobStore = {
    val storeFolder = rootFolder + "/" + name
    new S3BlobStore(this.s3ClientFactory, this.bucketName, storeFolder)
  }
}
