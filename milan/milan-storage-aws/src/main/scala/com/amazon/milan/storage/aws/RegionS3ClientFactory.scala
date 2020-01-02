package com.amazon.milan.storage.aws

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client


/**
 * An [[S3ClientFactory]] that creates a single S3 client for the specified region and always returns that instance.
 *
 * @param region The region to create a client for.
 */
class RegionS3ClientFactory(region: String) extends S3ClientFactory {
  @transient private var s3Client: S3Client = _

  def this(region: Region) {
    this(region.id())
  }

  override def createClient(): S3Client = {
    if (this.s3Client == null) {
      this.s3Client = S3Client.builder()
        .region(Region.of(region))
        .credentialsProvider(DefaultCredentialsProvider.create())
        .build()
    }

    this.s3Client
  }
}
