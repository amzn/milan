package com.amazon.milan.storage.aws

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client


object S3ClientFactory {
  /**
   * Creates an [[S3ClientFactory]] that produces default S3 clients for the specified region using default credentials.
   *
   * @param region An AWS region.
   * @return An [[S3ClientFactory]] that produces clients for that region.
   */
  def createForRegion(region: Region): S3ClientFactory = {
    new RegionS3ClientFactory(region.id())
  }
}


/**
 * Interface for creating S3 clients.
 *
 * Serializable classes that use an [[S3Client]] client cannot have that client instance passed in their constructor
 * because [[S3Client]] instances are not necessarily serializable. Instead, these classes can take an
 * [[S3ClientFactory]] instance and use that to create S3 clients as necessary during their own initialization steps
 * after they are deserialized.
 */
trait S3ClientFactory extends Serializable {
  /**
   * Create an S3 client.
   *
   * @return An S3 client.
   */
  def createClient(): S3Client
}
