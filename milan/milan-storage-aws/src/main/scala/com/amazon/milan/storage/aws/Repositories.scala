package com.amazon.milan.storage.aws

import com.amazon.milan.manage.{ApplicationRepository, BlobStorePackageRepository, EntityStoreApplicationRepository, PackageRepository}
import com.amazon.milan.serialization.ScalaObjectMapper
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain

object Repositories {
  /**
   * Creates an [[ApplicationRepository]] that stores objects in S3.
   *
   * @param region     The AWS region where the bucket is located.
   * @param bucketName The name of the S3 bucket.
   * @param rootFolder The root folder of the repository in the bucket.
   * @return An [[ApplicationRepository]] interface to the repository in the bucket.
   */
  def createS3ApplicationRepository(region: Region, bucketName: String, rootFolder: String): ApplicationRepository = {
    val s3ClientFactory = S3ClientFactory.createForRegion(region)
    val entityStoreFactory = new S3EntityStoreFactory(s3ClientFactory, bucketName, rootFolder, ".json", new ScalaObjectMapper())
    new EntityStoreApplicationRepository(entityStoreFactory)
  }

  /**
   * Creates a [[PackageRepository]] that uses S3 for package storage.
   *
   * @param region     The AWS region where the bucket is located.
   * @param bucketName The S3 bucket name.
   * @param rootFolder The root folder of the package repository.
   * @return A [[PackageRepository]] interface to the repository in S3.
   */
  def createS3PackageRepository(region: Region, bucketName: String, rootFolder: String): PackageRepository = {
    val blobStore = S3BlobStore.create(region, bucketName, rootFolder)
    new BlobStorePackageRepository(blobStore)
  }

  /**
   * Creates a [[PackageRepository]] that uses S3 for package storage.
   *
   * @param region     The name of the AWS region where the bucket is located.
   * @param bucketName The S3 bucket name.
   * @param rootFolder The root folder of the package repository.
   * @return A [[PackageRepository]] interface to the repository in S3.
   */
  def createS3PackageRepository(region: String, bucketName: String, rootFolder: String): PackageRepository = {
    val blobStore = S3BlobStore.create(region, bucketName, rootFolder)
    new BlobStorePackageRepository(blobStore)
  }

  /**
   * Creates a [[PackageRepository]] that uses S3 for package storage.
   *
   * @param bucket     The name of the S3 bucket.
   * @param rootFolder The root folder of the package repository.
   * @return A [[PackageRepository]] interface to the repository in S3.
   */
  def createS3PackageRepository(bucket: String, rootFolder: String): PackageRepository = {
    val region = new DefaultAwsRegionProviderChain().getRegion
    this.createS3PackageRepository(region, bucket, rootFolder)
  }
}
