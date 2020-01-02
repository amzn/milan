package com.amazon.milan.flink.control

import java.nio.file.{Path, Paths}
import java.util.regex.Pattern

import com.amazon.milan.manage._
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


object ApplicationController {
  val APP_RUNNER_CLASS_NAME: String = com.amazon.milan.flink.apps.SerializedApplicationRunner.getClass.getName.replace("$", "")
  val DASHBOARD_RUNNER_CLASS_NAME: String = com.amazon.milan.flink.apps.SerializedApplicationDashboardRunner.getClass.getName.replace("$", "")
  val APPLICATION_RESOURCE_NAME: String = "application.json"
}


class ApplicationController(packageRepository: PackageRepository,
                            flinkClient: FlinkClient,
                            packageCachePath: String,
                            region: String,
                            cloudFormationCachePath: String) extends Serializable {

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  def this(packageRepository: PackageRepository,
           flinkClient: FlinkClient,
           packageCachePath: Path,
           region: String,
           cloudFormationCachePath: Path) {
    this(packageRepository, flinkClient, packageCachePath.toString, region, cloudFormationCachePath.toString)
  }

  /**
   * Starts a packaged application.
   *
   * @param applicationPackageId The ID of the package in the package repository.
   * @param snapshotPath         Directory where the snapshot should be stored.
   * @param createDashboard      Flag to control whether dashboard is created.
   * @return An ID that identifies the running application instance.
   */
  def startApplicationPackage(applicationPackageId: String, snapshotPath: Option[String] = None, createDashboard: Boolean = false): String = {
    val jarPath = Paths.get(this.packageCachePath).resolve(applicationPackageId + ".jar")

    if (jarPath.toFile.exists()) {
      this.logger.info(s"Package '$jarPath' already exists, using cached version.")
    }
    else {
      this.logger.info(s"Copying package to '$jarPath'.")
      this.packageRepository.copyToFile(applicationPackageId, jarPath)
    }

    val flinkApplicationId =
      try {
        if (snapshotPath.isDefined) {
          this.flinkClient.runFromSavepoint(jarPath, snapshotPath.get, ApplicationController.APP_RUNNER_CLASS_NAME, "--application-resource-name", s"/${ApplicationController.APPLICATION_RESOURCE_NAME}")
        }
        else {
          this.flinkClient.run(jarPath, ApplicationController.APP_RUNNER_CLASS_NAME,
            "--application-resource-name", s"/${ApplicationController.APPLICATION_RESOURCE_NAME}")
        }
      }
      catch {
        case ex: FlinkCommandException =>
          throw new StartApplicationException(s"Process completed with exit code ${ex.exitCode}.", ex)
      }

    if (createDashboard) {
      this.startDashboard(jarPath, flinkApplicationId)
    }

    this.logger.info(s"Application package '$applicationPackageId' started as Flink application '$flinkApplicationId'${if (snapshotPath.isDefined) s" from snapshot '${snapshotPath.get}'." else "."}")
    flinkApplicationId
  }

  /**
   * Starts dashboard from serialized application jar.
   *
   * @param jarPath            Path to the serialized application jar.
   * @param flinkApplicationId The ID of the Flink application.
   */
  private def startDashboard(jarPath: Path, flinkApplicationId: String): Unit = {
    val output = ProcessCommandExecutor.execute(s"java -classpath ${jarPath.toString}:${this.getFlinkJarPath} ${ApplicationController.DASHBOARD_RUNNER_CLASS_NAME} --application-resource-name /${ApplicationController.APPLICATION_RESOURCE_NAME} --application-instance-id $flinkApplicationId --region $region --cloudformation-cache-folder $cloudFormationCachePath")

    if (output.exitCode != 0) {
      this.logger.error("Could not generate CloudFormation template for dashboard.")
      throw new StartApplicationException(s"Process completed with exit code ${output.exitCode}. \n${output.getFullErrorOutput}")
    }

    val templatePath = Paths.get(this.cloudFormationCachePath).resolve(s"dashboard-$flinkApplicationId.json")
    this.logger.info(s"Created Cloudformation template at ${templatePath.toString}")
    val stackName = this.getDashboardStackName(flinkApplicationId)

    val executorResult = ProcessCommandExecutor.execute(s"aws --region $region cloudformation create-stack --template-body file://${templatePath.toString} --stack-name $stackName")
    if (executorResult.exitCode == 0) {
      this.logger.info(s"Created CloudFormation stack '$stackName'.")
    }
    else {
      this.logger.error(s"Could not create CloudFormation stack for dashboard. ${executorResult.getFullErrorOutput}")
    }
  }

  /**
   * Get Flink jar path on EMR node.
   *
   * @return Path of Flink jar.
   */
  private def getFlinkJarPath: String = {
    val output = ProcessCommandExecutor.execute("flink --version")
    if (output.exitCode != 0) {
      this.logger.error("Could not get Flink version.")
      throw new StartApplicationException(s"Process completed with exit code ${output.exitCode}. \n${output.getFullErrorOutput}")
    }

    val versionPattern = Pattern.compile("\\d\\.\\d\\.\\d")
    val matcher = versionPattern.matcher(output.getFullStandardOutput)
    if (!matcher.find()) {
      throw new StartApplicationException(s"Could not get Flink version from CLI output '${output.getFullStandardOutput}'.")
    }

    val flinkVersion = matcher.group()

    val scalaVersion = util.Properties.versionNumberString
    val scalaCompatVersion = scalaVersion.substring(0, scalaVersion.lastIndexOf("."))
    s"/usr/lib/flink/lib/flink-dist_$scalaCompatVersion-$flinkVersion.jar"
  }

  /**
   * Stops a running Flink application.
   *
   * @param flinkApplicationId The ID of the Flink application to stop.
   * @param deleteDashboard    Flag to control whether dashboard is deleted.
   */
  def stopApplicationInstance(flinkApplicationId: String, deleteDashboard: Boolean): Unit = {
    this.flinkClient.cancel(flinkApplicationId)

    val stackName = this.getDashboardStackName(flinkApplicationId)

    if (deleteDashboard) {
      val executorResult = new ProcessCommandExecutor().executeAndWait(s"aws --region $region cloudformation delete-stack --stack-name $stackName")
      if (executorResult.exitCode == 0) {
        this.logger.info(s"Deleted CloudFormation stack '$stackName'.")
      }
      else {
        this.logger.error(s"Could not delete CloudFormation stack for dashboard. ${executorResult.getFullErrorOutput}")
      }
    }

  }

  /**
   * Get name of dashboard stack.
   *
   * @param applicationInstanceId The ID of the Flink application associated with the stack.
   * @return Name of the dashboard stack.
   */
  private def getDashboardStackName(applicationInstanceId: String): String = s"Dashboard-Stack-$applicationInstanceId"

  /**
   * Gets the application IDs of all running Flink applications.
   *
   * @return An array of Flink application IDs.
   */
  def listRunningApplications(): Array[String] = {
    this.flinkClient.listRunning()
  }

  /**
   * Create snapshot for a Flink application
   *
   * @param flinkApplicationId The ID of the Flink application for which snapshot should be created.
   * @param targetDirectory    Directory where the snapshot should be stored.
   *                           The location has to be accessible by both the JobManager(s) and TaskManager(s).
   *                           Can be an S3 path for example.
   * @return Snapshot path
   */
  def snapshotApplication(flinkApplicationId: String, targetDirectory: String): Option[String] = {
    try {
      this.logger.info(s"Calling flink client to snapshot application $flinkApplicationId at $targetDirectory")
      val snapshotPath = this.flinkClient.snapshot(flinkApplicationId, targetDirectory)
      this.logger.info(s"Snapshot created. Path: $snapshotPath")
      snapshotPath
    }
    catch {
      case e: FlinkCommandException =>
        this.logger.error(s"Error executing Flink command to create snapshot for application '$flinkApplicationId' at '$targetDirectory'", e)
        throw e
    }
  }
}
