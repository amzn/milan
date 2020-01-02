package com.amazon.milan.flink.control

import java.time.Instant

import com.amazon.milan.control._
import com.amazon.milan.manage.ProcessCommandExecutor
import com.amazon.milan.serialization.ScalaObjectMapper
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


/**
 * Wraps an [[ApplicationController]] object and handles instructions that are sent via JSON objects.
 *
 * @param applicationController The [[ApplicationController]] that will be used to execute the commands.
 */
class ApplicationControllerMessageHandler(applicationController: ApplicationController)
  extends Serializable {

  @transient private lazy val objectMapper = new ScalaObjectMapper()
  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))
  private var applications: List[ApplicationInfo] = List()

  /**
   * Process a controller message.
   *
   * @param messageBodyJson JSON containing the serialized message.
   */
  def processMessage(messageBodyJson: String): ProcessMessageResult = {
    val message = this.objectMapper.readValue[ApplicationControllerMessage](messageBodyJson, classOf[ApplicationControllerMessage])

    this.logger.info(s"Handling message of type ${message.messageType}, body: $messageBodyJson")

    try {
      message.messageType match {
        case MessageType.StartApplication =>
          this.startApplication(messageBodyJson)

        case MessageType.StopApplication =>
          this.stopApplication(messageBodyJson)

        case MessageType.SnapshotApplication =>
          this.snapshotApplication(messageBodyJson)

        case MessageType.RestoreApplication =>
          this.restoreApplication(messageBodyJson)

        case MessageType.ShellCommand =>
          this.shellCommand(messageBodyJson)

        case unknownMessageType =>
          throw new IllegalArgumentException(s"Message type '$unknownMessageType' not recognized.")
      }
    }
    catch {
      case ex: Exception =>
        this.logger.error(s"Error handling ${message.messageType} message.", ex)
        throw ex
    }
  }

  /**
   * Handles a StartApplication message.
   *
   * @param messageBodyJson The json-encoded message object.
   * @return The result from processing the message.
   */
  private def startApplication(messageBodyJson: String): ProcessMessageResult = {
    val message = this.objectMapper.readValue(messageBodyJson, classOf[StartApplicationMessage])

    this.logger.info(s"Starting application with instance ID '${message.applicationInstanceId}'.")

    val flinkApplicationId = this.applicationController.startApplicationPackage(message.applicationPackageId,
      createDashboard = message.createDashboard)

    this.logger.info(s"Application package '${message.applicationPackageId}' started as Flink application '$flinkApplicationId'.")

    val newApplicationInfo = new ApplicationInfo(
      message.applicationInstanceId,
      flinkApplicationId,
      message.applicationPackageId)

    this.applications = this.applications :+ newApplicationInfo

    new ProcessMessageResult(true, "")
  }

  /**
   * Handles a StopApplication message.
   *
   * @param messageBodyJson The json-encoded message object.
   * @return The result from processing the message.
   */
  private def stopApplication(messageBodyJson: String): ProcessMessageResult = {
    val message = this.objectMapper.readValue(messageBodyJson, classOf[StopApplicationMessage])

    this.logger.info(s"Stopping application with instance ID '${message.applicationInstanceId}'.")

    this.applications.find(app => app.applicationInstanceId == message.applicationInstanceId) match {
      case Some(appInfo) =>
        this.applicationController.stopApplicationInstance(appInfo.controllerApplicationId, message.deleteDashboard)
        new ProcessMessageResult(true, "")

      case None =>
        val errorDetails = s"Couldn't find application with application instance ID '${message.applicationInstanceId}' in the list of running applications."
        this.logger.error(errorDetails)
        new ProcessMessageResult(false, errorDetails)
    }
  }

  /**
   * Handles a SnapshotApplication message.
   *
   * @param messageBodyJson The json-encoded message object.
   * @return The result from processing the message.
   */
  private def snapshotApplication(messageBodyJson: String): ProcessMessageResult = {
    val message = this.objectMapper.readValue(messageBodyJson, classOf[SnapshotApplicationMessage])

    this.logger.info(s"Creating snapshot for application with instance ID '${message.applicationInstanceId}'.")

    this.applications.find(app => app.applicationInstanceId == message.applicationInstanceId) match {
      case Some(appInfo) =>
        try {
          this.applicationController.snapshotApplication(appInfo.controllerApplicationId, message.targetDirectory)
          new ProcessMessageResult(true, "")
        }
        catch {
          case e: FlinkCommandException =>
            this.logger.error(s"Error handling snapshot message for flinkApplicationId: '${appInfo.controllerApplicationId}' and targetDirectory: '${message.targetDirectory}'", e)
            new ProcessMessageResult(false, e.errorOutput)
        }

      case None =>
        val errorDetails = s"Couldn't find application with application instance ID '${message.applicationInstanceId}' in the list of running applications."
        this.logger.error(errorDetails)
        new ProcessMessageResult(false, errorDetails)
    }
  }

  /**
   * Handles a RestoreApplication message
   *
   * @param messageBodyJson The json-encoded message object.
   * @return The result from processing the message.
   */
  private def restoreApplication(messageBodyJson: String): ProcessMessageResult = {

    val message = this.objectMapper.readValue(messageBodyJson, classOf[RestoreApplicationMessage])

    this.logger.info(s"Restoring application with instance ID '${message.applicationInstanceId}' from snapshot '${message.snapshotPath}'.")

    val flinkApplicationId = this.applicationController.startApplicationPackage(message.applicationPackageId,
      Some(message.snapshotPath), message.createDashboard)

    this.logger.info(s"Application package '${message.applicationPackageId}' restored as Flink application '$flinkApplicationId' from snapshotPath '${message.snapshotPath}.")

    val newApplicationInfo = new ApplicationInfo(
      message.applicationInstanceId,
      flinkApplicationId,
      message.applicationPackageId)

    this.applications = this.applications :+ newApplicationInfo

    new ProcessMessageResult(true, "")
  }

  /**
   * Handles a ShellCommand message.
   *
   * @param messageBodyJson The json-encoded message object.
   * @return The result from processing the message.
   */
  private def shellCommand(messageBodyJson: String): ProcessMessageResult = {
    val message = this.objectMapper.readValue(messageBodyJson, classOf[ShellCommandMessage])

    this.logger.info(s"Executing shell command '${message.shellCommand}'.")

    val commandExecutor = new ProcessCommandExecutor()
    val output = commandExecutor.executeAndWait(message.shellCommand)

    if (output.exitCode != 0) {
      new ProcessMessageResult(false, s"Command: '${message.shellCommand}'\nError output: ${output.getFullErrorOutput}")
    }
    else {
      new ProcessMessageResult(true, s"Command: '${message.shellCommand}'\nStandard output: ${output.getFullStandardOutput}")
    }
  }

  /**
   * Check for any changes to the running Flink applications that were started by this controller.
   * Running Flink applications that were not started by this controller are ignored.
   *
   * @return True if any changes were detected, otherwise False.
   */
  def checkForChanges(): Boolean = {
    try {
      val actualApplicationIds = this.applicationController.listRunningApplications().toSet
      this.logger.info(s"Running application IDs: ${actualApplicationIds.mkString("'", ", ", "'")}")

      val cachedApplicationIds = this.applications.map(info => info.controllerApplicationId).toSet
      this.logger.info(s"Cached application IDs: ${cachedApplicationIds.mkString("'", ", ", "'")}")

      val stoppedApplicationIds = cachedApplicationIds -- actualApplicationIds

      if (stoppedApplicationIds.isEmpty) {
        // No stopped applications so return false.
        false
      }
      else {
        this.logger.info(s"Detected stopped Flink applications: ${stoppedApplicationIds.mkString("'", ", ", "'")}")

        // Retain all applications where the Flink application ID is not in the set of stopped applications.
        this.applications = this.applications.filterNot(info => stoppedApplicationIds.contains(info.controllerApplicationId))

        // Return true because there were stopped applications.
        true
      }
    }
    catch {
      case ex: Exception =>
        this.logger.error("Error checking for changes to running applications.", ex)
        throw ex
    }
  }

  /**
   * Gets the current state of the controller.
   *
   * @param controllerId The ID of this controller.
   * @return An [[ApplicationControllerState]] containing the state of the controller.
   */
  def saveState(controllerId: String): ApplicationControllerState = {
    new ApplicationControllerState(controllerId, this.applications.toArray)
  }

  /**
   * Loads controller state from a previously saved state.
   *
   * @param state Controller state to load.
   */
  def loadState(state: ApplicationControllerState): Unit = {
    this.applications = state.applicationInfo.toList
  }

  /**
   * Create snapshot for applications in controller's cache
   *
   * @param snapshotBaseDirectory Base directory where the snapshots should be stored
   */
  def snapshotApplications(snapshotBaseDirectory: String): Unit = {
    this.applications.foreach { application =>
      this.logger.info(s"Calling snapshotApplication for application id ${application.controllerApplicationId} with instance id ${application.applicationInstanceId}")
      val snapshotDirectory = s"$snapshotBaseDirectory/${application.applicationInstanceId}/${Instant.now}"
      try {
        this.applicationController.snapshotApplication(application.controllerApplicationId, snapshotDirectory)
      }
      catch {
        case e: FlinkCommandException =>
          //Log error and move on to the next application
          this.logger.error(s"Failed to create snapshot for application '${application.controllerApplicationId}' at '$snapshotDirectory'", e)
      }
    }
  }
}


class ProcessMessageResult(var success: Boolean, var details: String) extends Serializable {
}
