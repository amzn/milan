package com.amazon.milan.flink.manage

import java.nio.file.{Files, Path}

import com.amazon.milan.control.aws.ControllerClient
import com.amazon.milan.flink.control.LocalFlinkControllerClient
import com.amazon.milan.manage._
import com.amazon.milan.storage.aws.Repositories
import software.amazon.awssdk.regions.Region


object FlinkApplicationManager {
  /**
   * Creates an [[ApplicationManager]] that stores application artifacts in memory and executes applications using the
   * Flink in-process mini cluster.
   *
   * @return An [[ApplicationManager]].
   */
  def createMemoryApplicationManager(): ApplicationManager = {
    val applicationRepository = ApplicationRepository.createMemoryApplicationRepository()
    val packager = new InProcessPackagerAndController()
    new ApplicationManager(applicationRepository, packager, packager)
  }

  /**
   * Creates an [[ApplicationManager]] that executes applications using the local flink cluster.
   * This relies on invoking the "flink" command, so it must be on the path.
   *
   * @param rootPath The root path to use for the local package and application repositories and the package cache.
   * @return An [[ApplicationManager]].
   */
  def createLocalFlinkApplicationManager(rootPath: Path): ApplicationManager = {
    val appRepoPath = rootPath.resolve("apprepo/")
    val packageRepoPath = rootPath.resolve("packageRepo/")
    val packageCachePath = rootPath.resolve(".packages/")

    Files.createDirectory(appRepoPath)
    Files.createDirectory(packageRepoPath)
    Files.createDirectory(packageCachePath)

    val applicationRepository = ApplicationRepository.createLocalFolderApplicationRepository(appRepoPath)
    val packager = JarApplicationPackager.createForCurrentJar(packageRepoPath)

    val packageRepository = new LocalPackageRepository(packageRepoPath)
    val controllerClient = new LocalFlinkControllerClient(packageRepository, packageCachePath)

    new ApplicationManager(applicationRepository, packager, controllerClient)
  }

  /**
   * Creates an [[ApplicationManager]] that stores application artifacts in S3 and controls an application controller
   * via Kinesis streams.
   *
   * @param region                      The AWS region where the resources are located.
   * @param s3BucketName                The name of the S3 bucket containing the application and package repositories.
   * @param s3RootFolder                The root folder in the bucket for the repositories.
   * @param controllerMessageStreamName The name of the Kinesis stream where application controller messages are sent.
   * @param controllerStateStreamName   The name of the Kinesis stream where the application controller places state information.
   * @return An [[ApplicationManager]].
   */
  def createS3KinesisApplicationManager(region: Region,
                                        s3BucketName: String,
                                        s3RootFolder: String,
                                        controllerMessageStreamName: String,
                                        controllerStateStreamName: String): ApplicationManager = {
    val applicationRepository = Repositories.createS3ApplicationRepository(region, s3BucketName, s3RootFolder + "/apprepo")
    val packageRepository = Repositories.createS3PackageRepository(region, s3BucketName, s3RootFolder + "/packagerepo")
    val packager = JarApplicationPackager.createForCurrentJar(packageRepository)
    val controllerClient = ControllerClient.createForKinesisStreams(
      controllerMessageStreamName,
      controllerStateStreamName,
      region = region)
    new ApplicationManager(applicationRepository, packager, controllerClient)
  }
}
