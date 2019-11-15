package com.amazon.milan.control

/**
 * Contains the different message types.
 * Using strings instead of an enum because Jackson doesn't handle scala enum serialization very well.
 */
object MessageType {
  val StartApplication = "StartApplication"
  val StopApplication = "StopApplication"
  val SnapshotApplication = "SnapshotApplication"
  val RestoreApplication = "RestoreApplication"
  val ShellCommand = "ShellCommand"
}


/**
 * Base class for application controller messages.
 *
 * @param messageType The type of message. This should be one of the [[MessageType]] strings.
 */
class ApplicationControllerMessage(val messageType: String)
  extends Serializable {
}


/**
 * A message that instructs the controller to start an application.
 *
 * @param applicationInstanceId The instance ID to assign to the started application.
 * @param applicationPackageId  The ID of the package containing the application.
 * @param createDashboard       Flag to control whether dashboard is created.
 */
class StartApplicationMessage(val applicationInstanceId: String,
                              val applicationPackageId: String,
                              val createDashboard: Boolean = false)
  extends ApplicationControllerMessage(MessageType.StartApplication) {

  def this() {
    this("", "", false)
  }
}


/**
 * A message that instructs the controller to terminate an application.
 *
 * @param applicationInstanceId The instance ID of the application to terminate.
 * @param deleteDashboard       Flag to control whether dashboard is deleted.
 */
class StopApplicationMessage(val applicationInstanceId: String,
                             val deleteDashboard: Boolean = true)
  extends ApplicationControllerMessage(MessageType.StopApplication) {

  def this() {
    this("", true)
  }
}


/**
 * A message that instructs the controller to snapshot an application.
 *
 * @param applicationInstanceId The instance ID of the application to snapshot.
 * @param targetDirectory       Directory where the snapshot should be stored.
 *                              The location has to be accessible by both the JobManager(s) and TaskManager(s).
 *                              Can be an S3 path for example.
 */
class SnapshotApplicationMessage(val applicationInstanceId: String, val targetDirectory: String)
  extends ApplicationControllerMessage(MessageType.SnapshotApplication) {

  def this() {
    this("", "")
  }
}

/**
 * A message that instructs the controller to restore an application from snapshot.
 *
 * @param applicationInstanceId The instance ID to assign to the restored application.
 * @param applicationPackageId  The ID of the package containing the application.
 * @param snapshotPath          Absolute path to either the snapshot’s directory or the _metadata file.
 * @param createDashboard       Flag to control whether dashboard is created.
 */
class RestoreApplicationMessage(val applicationInstanceId: String, val applicationPackageId: String,
                                val snapshotPath: String, val createDashboard: Boolean = false)
  extends ApplicationControllerMessage(MessageType.RestoreApplication) {

  def this() {
    this("", "", "")
  }
}


/**
 * A message that instructs the controller to execute a shell command and output the result.
 * This message is only here for testing and should be disabled or otherwise protected in production deployments.
 *
 * @param shellCommand The shell command to execute.
 */
class ShellCommandMessage(val shellCommand: String)
  extends ApplicationControllerMessage(MessageType.ShellCommand) {

  def this() {
    this("")
  }
}
