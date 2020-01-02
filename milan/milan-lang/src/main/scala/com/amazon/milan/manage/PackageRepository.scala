package com.amazon.milan.manage

import java.io.{InputStream, OutputStream}
import java.nio.file.Path

import com.amazon.milan.storage.MemoryBlobStore


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
