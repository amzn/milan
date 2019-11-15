package com.amazon.milan.control.client


trait ApplicationControllerClient {
  /**
   * Instructs the controller to start an application.
   *
   * @param applicationPackageId The ID of the application package.
   * @param createDashboard      Flag to control whether dashboard is created.
   * @return An ID identifying the running application instance.
   */
  def startApplication(applicationPackageId: String, createDashboard: Boolean = false): String

  /**
   * Instructs the controller to terminate an application instance.
   *
   * @param applicationInstanceId The ID of the application instance.
   * @param deleteDashboard       Flag to control whether dashboard is deleted.
   */
  def stopApplication(applicationInstanceId: String, deleteDashboard: Boolean = true): Unit

  /**
   * Instructs the controller to snapshot an application instance.
   *
   * @param applicationInstanceId The ID of the application instance.
   * @param targetDirectory       Directory where the snapshot should be stored.
   *                              The location has to be accessible by both the JobManager(s) and TaskManager(s).
   *                              Can be an S3 path for example.
   */
  def snapshotApplication(applicationInstanceId: String, targetDirectory: String): Unit

  /**
   * Instructs the controller to restore an application from snapshot.
   *
   * @param applicationPackageId The ID of the application package.
   * @param snapshotPath         Absolute path to either the snapshot’s directory or the _metadata file.
   * @return An ID identifying the running application instance restored from snapshot.
   */
  def restoreApplication(applicationPackageId: String, snapshotPath: String): String

  /**
   * Gets the list of applications that are currently running on the controller.
   *
   * @return An array of application instance IDs.
   */
  def listRunningApplications(): Array[RunningApplicationInstance]
}


/**
 * Contains information about a running application.
 *
 * @param applicationInstanceId The application instance ID.
 * @param applicationPackageId  The ID of the packaged that contains the application.
 */
case class RunningApplicationInstance(applicationInstanceId: String, applicationPackageId: String) {
  override def toString: String = s"{ applicationInstanceId: $applicationInstanceId, applicationPackageId: $applicationPackageId }"
}
