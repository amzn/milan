package com.amazon.milan.manage

import java.io.FileInputStream
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{FileSystems, Files, Path, Paths}
import java.util

import com.amazon.milan.Id
import com.amazon.milan.application.ApplicationInstance
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


object JarApplicationPackager {
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  def createForCurrentJar(packageFolder: Path): JarApplicationPackager = {
    val packageRepository = new LocalPackageRepository(packageFolder)
    JarApplicationPackager.createForCurrentJar(packageRepository)
  }

  def createForCurrentJar(packageRepository: PackageRepository): JarApplicationPackager = {
    val classPath = getClass.getProtectionDomain.getCodeSource.getLocation.getFile
    val jarLoc = classPath.toLowerCase.indexOf(".jar")
    val currentJarPath = Paths.get(classPath.substring(0, jarLoc + 4))

    new JarApplicationPackager(currentJarPath, packageRepository, "application.json")
  }

  /**
   * Creates a jar containing a serialized ApplicationDefinition as well as all of the functionality necessary to run
   * the application.
   *
   * @param instance                The application to package.
   * @param outputPath              The path of the output jar.
   * @param applicationResourceName The name of the resource containing the serialized application in the output jar.
   */
  def packageApplication(instance: ApplicationInstance,
                         outputPath: Path,
                         applicationResourceName: String): Unit = {
    val classPath = getClass.getProtectionDomain.getCodeSource.getLocation.getFile
    val jarLoc = classPath.toLowerCase.indexOf(".jar")
    val currentJarPath = Paths.get(classPath.substring(0, jarLoc + 4))
    JarApplicationPackager.packageApplication(instance, outputPath, applicationResourceName, currentJarPath)
  }

  /**
   * Creates a jar containing a serialized ApplicationDefinition as well as all of the functionality necessary to run
   * the application.
   *
   * @param instance                The application instance definition to package.
   * @param outputPath              The path of the output jar.
   * @param applicationResourceName The name of the resource containing the serialized application in the output jar.
   */
  def packageApplication(instance: ApplicationInstance,
                         outputPath: Path,
                         applicationResourceName: String,
                         templateJarPath: Path): Unit = {
    if (!Files.exists(templateJarPath)) {
      throw new IllegalArgumentException(s"Template jar file '$templateJarPath' does not exist.")
    }

    this.logger.info(s"Copying template jar '$templateJarPath' to '$outputPath'.")

    Files.copy(templateJarPath, outputPath)

    this.logger.info("Opening output jar.")
    val destUri = URI.create("jar:file:" + outputPath.toAbsolutePath.toString)
    val fs = FileSystems.newFileSystem(destUri, new util.HashMap[String, String]())

    try {
      val applicationOutputPath = fs.getPath(s"/$applicationResourceName")
      Files.deleteIfExists(applicationOutputPath)

      this.logger.info(s"Saving application to jar as '$applicationOutputPath'.")
      Files.write(applicationOutputPath, instance.toJsonString.getBytes(StandardCharsets.UTF_8))
    }
    finally {
      fs.close()
    }
  }
}


/**
 * Methods for packaging an ApplicationDefinition into a jar that can be executed as a Flink application.
 *
 * @param templateJarPath         The jar to use as a template for output jars.
 * @param packageRepository       The repository where packages will be stored.
 * @param applicationResourceName The name of the JAR resource that will contain the serialized application instance.
 */
class JarApplicationPackager(val templateJarPath: Path,
                             val packageRepository: PackageRepository,
                             val applicationResourceName: String = "application.json") extends ApplicationPackager {

  private val logger = Logger(LoggerFactory.getLogger(getClass))

  override def packageApplication(instanceDefinition: ApplicationInstance): String = {
    val temporaryPackageFile = Files.createTempFile("milan-application-package", ".jar")

    try {
      this.logger.info(s"Writing package to temporary file '$temporaryPackageFile'.")

      Files.deleteIfExists(temporaryPackageFile)

      val packageId = Id.newId()

      JarApplicationPackager.packageApplication(
        instanceDefinition,
        temporaryPackageFile,
        this.applicationResourceName,
        this.templateJarPath)

      val packageInputStream = new FileInputStream(temporaryPackageFile.toFile)

      try {
        this.logger.info(s"Copying package from '$temporaryPackageFile' to repository.")
        this.packageRepository.copyToRepository(packageId, packageInputStream)
        packageId
      }
      finally {
        packageInputStream.close()
      }
    }
    finally {
      this.logger.info(s"Deleting temporary package file '$temporaryPackageFile'.")
      Files.deleteIfExists(temporaryPackageFile)
    }
  }
}
