package com.amazon.milan.manage

import java.io.ByteArrayOutputStream

import com.amazon.milan.Id
import com.amazon.milan.control.client.{ApplicationControllerClient, RunningApplicationInstance}


/**
 * An implementation of [[ApplicationControllerClient]] that does nothing except verify that the package exists, and
 * maintain a list of running applications.
 *
 * @param packageRepository The package repository.
 */
class NoOpControllerClient(packageRepository: PackageRepository) extends ApplicationControllerClient {
  private var instances = List[RunningApplicationInstance]()

  override def startApplication(applicationPackageId: String, createDashboard: Boolean = false): String = {
    val packageBytes = new ByteArrayOutputStream()
    this.packageRepository.copyToStream(applicationPackageId, packageBytes)

    val instanceId = Id.newId()

    this.instances ::= RunningApplicationInstance(instanceId, applicationPackageId)

    instanceId
  }

  override def stopApplication(applicationInstanceId: String, deleteDashboard: Boolean = true): Unit = {
    this.instances = this.instances.filterNot(_.applicationInstanceId == applicationInstanceId)
  }

  override def listRunningApplications(): Array[RunningApplicationInstance] = {
    this.instances.toArray
  }

  override def snapshotApplication(applicationInstanceId: String, targetDirectory: String): Unit = {}

  override def restoreApplication(applicationPackageId: String, snapshotPath: String): String = {
    //This is a "no-op" client that just maintains a list of pretend-running applications
    //So, restoreApplication and startApplication in this context are the same
    this.startApplication(applicationPackageId)
  }
}
