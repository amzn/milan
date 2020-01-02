package com.amazon.milan.storage

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{ListObjectsV2Request, S3Object}

import scala.collection.JavaConverters._


package object aws {

  implicit class S3ClientExtensions(s3Client: S3Client) {
    /**
     * Lists all objects in an S3 bucket with a given prefix.
     *
     * @param bucket An S3 bucket.
     * @param prefix An object key prefix.
     * @return A [[TraversableOnce]] that yields all of the objects with the prefix in the bucket.
     */
    def listAllObjects(bucket: String, prefix: String): TraversableOnce[S3Object] = {
      val request = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build()
      val iterable = this.s3Client.listObjectsV2Paginator(request)
      iterable.iterator().asScala.flatMap(_.contents().iterator().asScala)
    }
  }

}
