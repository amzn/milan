package com.amazon.milan.manage

import java.io.{InputStream, OutputStream}
import java.nio.file.{Files, Path, StandardOpenOption}

import com.amazon.milan.storage.BlobStore


/**
 * A [[PackageRepository]] that uses a [[BlobStore]] for package storage.
 *
 * @param blobStore The blob store to use for storage.
 */
class BlobStorePackageRepository(blobStore: BlobStore) extends PackageRepository {
  override def copyToRepository(applicationPackageId: String, packageInputStream: InputStream): Unit = {
    this.blobStore.writeBlob(applicationPackageId, packageInputStream)
  }

  override def copyToStream(applicationPackageId: String, outputStream: OutputStream): Unit = {
    this.blobStore.readBlob(applicationPackageId, outputStream)
  }

  override def copyToFile(applicationPackageId: String, destinationPath: Path): Unit = {
    val outputStream = Files.newOutputStream(destinationPath, StandardOpenOption.CREATE)
    this.copyToStream(applicationPackageId, outputStream)
    outputStream.close()
  }
}
