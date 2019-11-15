package com.amazon.milan.manage

import java.io.{InputStream, OutputStream}
import java.nio.file.{Files, Path, Paths}


/**
 * An implementation of [[PackageRepository]] that uses a local folder for package storage.
 *
 * @param packageRepositoryPath The path to the repository folder.
 */
class LocalPackageRepository(packageRepositoryPath: String)
  extends PackageRepository {

  def this(packageRepositoryPath: Path) {
    this(packageRepositoryPath.toString)
  }

  override def copyToStream(applicationPackageId: String, outputStream: OutputStream): Unit = {
    this.ensureFolderExists()

    val packageFile = this.getPackageFile(applicationPackageId)
    Files.copy(packageFile, outputStream)
  }

  override def copyToRepository(applicationPackageId: String, packageInputStream: InputStream): Unit = {
    this.ensureFolderExists()

    val packageFile = this.getPackageFile(applicationPackageId)
    Files.copy(packageInputStream, packageFile)
  }

  override def copyToFile(applicationPackageId: String, destinationPath: Path): Unit = {
    this.ensureFolderExists()

    val packageFile = this.getPackageFile(applicationPackageId)
    Files.copy(packageFile, destinationPath)
  }

  def getPackageFile(applicationPackageId: String): Path = {
    Paths.get(this.packageRepositoryPath).resolve(applicationPackageId + ".jar")
  }

  private def ensureFolderExists(): Unit = {
    if (Files.notExists(Paths.get(this.packageRepositoryPath))) {
      Files.createDirectories(Paths.get(this.packageRepositoryPath))
    }
  }
}
