package com.amazon.milan.flink.control

import java.nio.file.Path

import com.amazon.milan.control.client.{ApplicationControllerClient, RunningApplicationInstance}
import com.amazon.milan.manage.{PackageRepository, ProcessCommandExecutor}


/**
 * An [[ApplicationControllerClient]] that directly executes command using the local flink cluster.
 *
 * @param packageRepository The package repository.
 */
class LocalFlinkControllerClient(packageRepository: PackageRepository,
                                 packageCachePath: Path) extends ApplicationControllerClient {
  private val flinkClient = new CommandLineFlinkClient(new ProcessCommandExecutor())

  private val controller = new ApplicationController(packageRepository, this.flinkClient, packageCachePath, "eu-west-1", packageCachePath)

  private var packageIds = Map.empty[String, String]

  /**
   * Instructs the controller to start an application.
   *
   * @param applicationPackageId The ID of the application package.
   * @param createDashboard      Flag to control whether dashboard is created.
   * @return An ID identifying the running application instance.
   */
  override def startApplication(applicationPackageId: String, createDashboard: Boolean): String = {
    val flinkAppId = this.controller.startApplicationPackage(applicationPackageId)
    this.packageIds += (flinkAppId -> applicationPackageId)
    flinkAppId
  }

  /**
   * Instructs the controller to terminate an application instance.
   *
   * @param applicationInstanceId The ID of the application instance.
   * @param deleteDashboard       Flag to control whether dashboard is deleted.
   */
  override def stopApplication(applicationInstanceId: String, deleteDashboard: Boolean): Unit = {
    this.controller.stopApplicationInstance(applicationInstanceId, deleteDashboard)
    this.packageIds -= applicationInstanceId
  }

  /**
   * Instructs the controller to snapshot an application instance.
   *
   * @param applicationInstanceId The ID of the application instance.
   * @param targetDirectory       Directory where the snapshot should be stored.
   *                              The location has to be accessible by both the JobManager(s) and TaskManager(s).
   *                              Can be an S3 path for example.
   */
  override def snapshotApplication(applicationInstanceId: String, targetDirectory: String): Unit = {
    throw new NotImplementedError()
  }

  /**
   * Instructs the controller to restore an application from snapshot.
   *
   * @param applicationPackageId The ID of the application package.
   * @param snapshotPath         Absolute path to either the snapshot’s directory or the _metadata file.
   * @return An ID identifying the running application instance restored from snapshot.
   */
  override def restoreApplication(applicationPackageId: String, snapshotPath: String): String = {
    throw new NotImplementedError()
  }

  /**
   * Gets the list of applications that are currently running on the controller.
   *
   * @return An array of application instance IDs.
   */
  override def listRunningApplications(): Array[RunningApplicationInstance] = {
    controller.listRunningApplications().map(id => RunningApplicationInstance(id, this.packageIds(id)))
  }
}
