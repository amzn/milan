package com.amazon.milan.manage

import java.nio.file.{Files, Path}

import com.amazon.milan.Id._
import com.amazon.milan.SemanticVersion
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.control.client.{ApplicationControllerClient, StreamApplicationControllerClient}
import com.amazon.milan.flink.control.LocalFlinkControllerClient
import com.amazonaws.regions.Regions


object ApplicationManager {
  /**
   * Creates an [[ApplicationManager]] that stores application artifacts in memory and executes applications in
   * the current process.
   *
   * @return An [[ApplicationManager]].
   */
  def createMemoryApplicationManager(): ApplicationManager = {
    val applicationRepository = ApplicationRepository.createMemoryApplicationRepository()
    val packager = new InProcessPackagerAndController()
    new ApplicationManager(applicationRepository, packager, packager)
  }

  def createLocalApplicationManager(rootPath: Path): ApplicationManager = {
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
  def createS3KinesisApplicationManager(region: Regions,
                                        s3BucketName: String,
                                        s3RootFolder: String,
                                        controllerMessageStreamName: String,
                                        controllerStateStreamName: String): ApplicationManager = {
    val applicationRepository = ApplicationRepository.createS3ApplicationRepository(region, s3BucketName, s3RootFolder + "/apprepo")
    val packageRepository = PackageRepository.createS3PackageRepository(region, s3BucketName, s3RootFolder + "/packagerepo")
    val packager = JarApplicationPackager.createForCurrentJar(packageRepository)
    val controllerClient = StreamApplicationControllerClient.createForKinesisStreams(
      controllerMessageStreamName,
      controllerStateStreamName, region = region)
    new ApplicationManager(applicationRepository, packager, controllerClient)
  }
}


class ApplicationManager(val applicationRepository: ApplicationRepository,
                         val packager: ApplicationPackager,
                         val controllerClient: ApplicationControllerClient) {
  /**
   * Packages, registers, and starts an instance of a new version of an application.
   *
   * @param version     The version being started. This version must not already be present in the application repository.
   * @param application The application definition.
   * @param config      The application configuration for the application instance.
   * @return An ID identifying the started application instance.
   */
  def startNewVersion(version: SemanticVersion,
                      application: Application,
                      config: ApplicationConfiguration): String = {
    val applicationId = application.applicationId

    // If the application isn't registered then register it.
    // TODO: there is a race condition here, and the S3 API doesn't have a "put without overwriting" option so the only
    //  solution is to set ACLs which we won't bother with yet.
    if (!this.applicationRepository.applicationExists(applicationId)) {
      // The user didn't supply an application name so just use the ID as the name.
      val appRegistration = new ApplicationRegistration(applicationId, applicationId)

      this.applicationRepository.registerApplication(appRegistration)
    }

    val appVersionId = newId()

    // Register a new application version.
    val appVersion = new ApplicationVersionRegistration(appVersionId, applicationId, version)
    this.applicationRepository.registerVersion(appVersion, application)

    // Create and register the instance definition.
    val instance = new ApplicationInstance(application, config)
    val instanceDefinitionId = instance.instanceDefinitionId

    val instanceDefinitionRegistration = new ApplicationInstanceDefinitionRegistration(
      instanceDefinitionId,
      applicationId,
      appVersion.applicationVersionId)

    this.applicationRepository.registerInstanceDefinition(instanceDefinitionRegistration, config)

    // Create a package for the instance definition.
    val packageId = this.packager.packageApplication(instance)
    val packageRegistration = new ApplicationPackageRegistration(packageId, applicationId, instanceDefinitionId)
    this.applicationRepository.registerPackage(packageRegistration)

    val instanceId = this.controllerClient.startApplication(packageId)

    // Register the instance we just started.
    val instanceRegistration = new ApplicationInstanceRegistration(instanceId, applicationId, instanceDefinitionId, packageId)
    this.applicationRepository.registerInstance(instanceRegistration)

    instanceId
  }

  /**
   * Gets all running instances of an application.
   *
   * @param applicationId An application ID.
   * @return A list of [[ApplicationInstanceRegistration]] objects referring to the running instances of the application.
   */
  def getRunningApplicationInstances(applicationId: String): List[ApplicationInstanceRegistration] = {
    this.controllerClient
      .listRunningApplications()
      .map(runningInstance => this.applicationRepository.getInstanceRegistration(runningInstance.applicationInstanceId))
      .filter(instance => instance.applicationId == applicationId)
      .toList
  }

  /**
   * Gets the [[ApplicationInstanceRegistration]] for the latest version of the application.
   *
   * @param applicationId An application ID.
   * @return The [[ApplicationInstanceRegistration]] for the latest version. If more than one instance meets the
   *         requirements, an arbitrary instance from the available instances is returned.
   */
  def getLatestApplicationInstance(applicationId: String): ApplicationInstanceRegistration = {
    val latestVersion = this.applicationRepository.getLatestVersion(applicationId)
    val instanceDefinition = this.applicationRepository.listInstanceDefinitions(latestVersion.applicationVersionId).head
    this.applicationRepository.listInstances(instanceDefinition.instanceDefinitionId).head
  }

  /**
   * Gets the [[Application]] for an application instance.
   *
   * @param applicationInstance An application instance.
   * @return The [[Application]] registered for the instance.
   */
  def getApplicationDefinition(applicationInstance: ApplicationInstanceRegistration): Application = {
    val instanceDefinition = this.applicationRepository.getInstanceDefinitionRegistration(applicationInstance.instanceDefinitionId)
    val versionRegistration = this.applicationRepository.getVersionRegistration(instanceDefinition.applicationVersionId)
    this.applicationRepository.getApplicationDefinition(versionRegistration.applicationId, versionRegistration.version)
  }

  /**
   * Gets the [[ApplicationConfiguration]] for an application instance.
   *
   * @param applicationInstance An application instance.
   * @return The [[ApplicationConfiguration]] registered for the instance.
   */
  def getApplicationDataConfiguration(applicationInstance: ApplicationInstanceRegistration): ApplicationConfiguration = {
    this.applicationRepository.getApplicationConfiguration(applicationInstance.applicationId, applicationInstance.instanceDefinitionId)
  }
}
