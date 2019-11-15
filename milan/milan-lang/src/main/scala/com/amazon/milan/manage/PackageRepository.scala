package com.amazon.milan.manage

import java.io.{InputStream, OutputStream}
import java.nio.file.Path

import com.amazon.milan.storage.{MemoryBlobStore, S3BlobStore}
import com.amazonaws.regions.{DefaultAwsRegionProviderChain, Regions}
import com.amazonaws.services.s3.AmazonS3URI


object PackageRepository {
  /**
   * Creates a [[PackageRepository]] that stores packages in memory.
   *
   * @return A [[PackageRepository]] interface to the memory repository.
   */
  def createMemoryPackageRepository(): PackageRepository = {
    val blobStore = new MemoryBlobStore()
    new BlobStorePackageRepository(blobStore)
  }

  /**
   * Creates a [[PackageRepository]] that uses S3 for package storage.
   *
   * @param region     The AWS region where the bucket is located.
   * @param bucketName The S3 bucket name.
   * @param rootFolder The root folder of the package repository.
   * @return A [[PackageRepository]] interface to the repository in S3.
   */
  def createS3PackageRepository(region: Regions, bucketName: String, rootFolder: String): PackageRepository = {
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
   * @param s3Uri An [[AmazonS3URI]] pointing to the root folder of the package. If the URI does not contain a region
   *              then the current default region will be used.
   * @return A [[PackageRepository]] interface to the repository in S3.
   */
  def createS3PackageRepository(s3Uri: AmazonS3URI): PackageRepository = {
    val region = s3Uri.getRegion match {
      case null =>
        new DefaultAwsRegionProviderChain().getRegion
      case someRegion =>
        someRegion
    }

    createS3PackageRepository(region, s3Uri.getBucket, s3Uri.getKey)
  }
}


/**
 * Interface to a package repository.
 */
trait PackageRepository extends Serializable {
  /**
   * Uploads a package to the repository.
   *
   * @param applicationPackageId The ID of the application package to copy.
   * @param packageInputStream   The input stream from which the package can be read.
   */
  def copyToRepository(applicationPackageId: String, packageInputStream: InputStream): Unit

  /**
   * Copies a package to an output stream.
   *
   * @param applicationPackageId The ID of the application package to copy.
   * @param outputStream         The destination stream to copy to.
   */
  def copyToStream(applicationPackageId: String, outputStream: OutputStream): Unit

  /**
   * Copies a package to a local file.
   *
   * @param applicationPackageId The ID of the application package to copy.
   * @param destinationPath      The destination to copy to.
   */
  def copyToFile(applicationPackageId: String, destinationPath: Path): Unit
}
