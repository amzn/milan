package com.amazon.milan.experimentation

import java.nio.file.Files

import com.amazon.milan.SemanticVersion
import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.control.client.{ApplicationControllerClient, StreamApplicationControllerClient}
import com.amazon.milan.manage._
import com.amazonaws.regions.Regions


object ExperimentApplicationManager {
  /**
   * Creates an [[ExperimentApplicationManager]] that stores application artifacts in S3 and controls an application
   * controller via Kinesis streams.
   *
   * @param region                      The AWS region where the resources are located.
   * @param s3BucketName                The name of the S3 bucket containing the application and package repositories.
   * @param s3RootFolder                The root folder in the bucket for the repositories.
   * @param controllerMessageStreamName The name of the Kinesis stream where application controller messages are sent.
   * @param controllerStateStreamName   The name of the Kinesis stream where the application controller places state information.
   * @return An [[ExperimentApplicationManager]].
   */
  def createS3KinesisExperimentApplicationManager(region: Regions,
                                                  s3BucketName: String,
                                                  s3RootFolder: String,
                                                  controllerMessageStreamName: String,
                                                  controllerStateStreamName: String): ExperimentApplicationManager = {
    val applicationRepository = ApplicationRepository.createS3ApplicationRepository(region, s3BucketName, s3RootFolder + "/apprepo")
    val packageRepository = PackageRepository.createS3PackageRepository(region, s3BucketName, s3RootFolder + "/packagerepo")
    val controllerClient = StreamApplicationControllerClient.createForKinesisStreams(
      controllerMessageStreamName,
      controllerStateStreamName, region = region)
    new ExperimentApplicationManager(applicationRepository, packageRepository, controllerClient)
  }
}


class ExperimentApplicationManager(val applicationRepository: ApplicationRepository,
                                   val packageRepository: PackageRepository,
                                   val controllerClient: ApplicationControllerClient) {
  /**
   * Starts a new instance of an application using a different data configuration than was originally used.
   *
   * @param applicationInstance The application instance to clone.
   * @param applicationId       The application ID for the new instance.
   * @param version             The version of the new instance.
   * @param config              The data configuration to use for the new instance.
   * @return The application instance ID of the new instance.
   */
  def startClone(applicationInstance: ApplicationInstanceRegistration,
                 applicationId: String,
                 version: SemanticVersion,
                 config: ApplicationConfiguration): String = {
    // Download the package for the source application so that we can re-use it with the new data config.
    val sourcePackageTempFile = Files.createTempFile(s"milan-package-${applicationInstance.packageId}", ".jar")

    try {
      this.packageRepository.copyToFile(applicationInstance.packageId, sourcePackageTempFile)
      val packager = new JarApplicationPackager(sourcePackageTempFile, this.packageRepository)
      val instanceDef = this.applicationRepository.getInstanceDefinitionRegistration(applicationInstance.instanceDefinitionId)
      val versionDef = this.applicationRepository.getVersionRegistration(instanceDef.applicationVersionId)
      val app = this.applicationRepository.getApplicationDefinition(applicationInstance.applicationId, versionDef.version)

      val manager = new ApplicationManager(this.applicationRepository, packager, this.controllerClient)

      manager.startNewVersion(version, app, config)
    }
    finally {
      Files.delete(sourcePackageTempFile)
    }
  }
}
