package com.amazon.milan.storage

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}


/**
 * An [[S3ClientFactory]] that creates a single S3 client for the specified region and always returns that instance.
 *
 * @param region The region to create a client for.
 */
class RegionS3ClientFactory(region: String) extends S3ClientFactory {
  @transient private var s3Client: AmazonS3 = _

  def this(region: Regions) {
    this(region.getName)
  }

  override def createClient(): AmazonS3 = {
    if (this.s3Client == null) {
      this.s3Client = AmazonS3ClientBuilder.standard()
        .withRegion(region)
        .withCredentials(new DefaultAWSCredentialsProviderChain())
        .build()
    }

    this.s3Client
  }
}
