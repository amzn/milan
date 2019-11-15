package com.amazon.milan.manage

import com.amazon.milan.Id
import com.amazon.milan.application.ApplicationInstance
import com.amazon.milan.control.client
import com.amazon.milan.control.client.ApplicationControllerClient
import com.amazon.milan.flink.compiler.FlinkCompiler
import com.amazon.milan.serialization.ScalaObjectMapper
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory

import scala.collection.mutable


class InProcessPackagerAndController
  extends ApplicationPackager
    with ApplicationControllerClient {

  private val runningApplications = new mutable.HashMap[String, RunningApplicationInstance]()
  private val stoppedApplications = new mutable.HashMap[String, RunningApplicationInstance]()
  private val logger = Logger(LoggerFactory.getLogger(getClass))
  private val packages = new mutable.HashMap[String, ApplicationInstance]()

  override def packageApplication(instanceDefinition: ApplicationInstance): String = {
    val packageId = Id.newId()
    this.packages.put(packageId, instanceDefinition)
    packageId
  }

  override def startApplication(applicationPackageId: String, createDashboard: Boolean = false): String = {
    val instance =
      this.packages.get(applicationPackageId) match {
        case None => throw new IllegalArgumentException(s"Application package '$applicationPackageId' not found.")
        case Some(app) => app
      }

    val executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val instanceJson = ScalaObjectMapper.writeValueAsString(instance)

    FlinkCompiler.defaultCompiler.compileFromInstanceJson(instanceJson, executionEnvironment)
    val instanceId = Id.newId()

    val runtimeEnvironment = this
    val runnable = new Runnable {
      override def run(): Unit = runtimeEnvironment.runApplication(instanceId, executionEnvironment)
    }

    val executionThread = new Thread(runnable)

    // Store the running instance information.
    val runningInstance = RunningApplicationInstance(instanceId, applicationPackageId, instance, executionEnvironment, executionThread)
    this.runningApplications.put(instanceId, runningInstance)

    this.logger.info(s"Starting execution thread for instance '$instanceId' of package '$applicationPackageId'")
    executionThread.start()

    if (createDashboard) {
      this.logger.info("Cannot create CloudFormation dashboards for applications running in process.")
    }

    instanceId
  }

  private def runApplication(instanceId: String, env: StreamExecutionEnvironment): Unit = {
    this.logger.info(s"Executing environment for application instance '$instanceId'.")

    env.execute()

    this.logger.info(s"Execution finished for application instance '$instanceId'.")

    this.runningApplications.get(instanceId) match {
      case Some(instance) =>
        this.logger.info(s"Moving application instance '$instanceId' from running instances collection to stopped.")
        this.runningApplications.remove(instanceId)
        this.stoppedApplications.put(instanceId, instance)

      case None =>
        this.logger.warn(s"Application instance '$instanceId' already removed from running instances collection.")
    }
  }

  override def stopApplication(applicationInstanceId: String, deleteDashboard: Boolean = true): Unit = {
    val instance = this.runningApplications(applicationInstanceId)

    if (instance.executionThread.isAlive) {
      instance.executionThread.interrupt()
    }

    this.runningApplications.remove(applicationInstanceId)
  }

  override def listRunningApplications(): Array[client.RunningApplicationInstance] = {
    this.runningApplications.values.map(
      app => client.RunningApplicationInstance(app.instanceId, app.packageId)).toArray
  }

  override def snapshotApplication(applicationInstanceId: String, targetDirectory: String): Unit = {
    // This class deals with applications running in-process whereas snapshots are supported only for applications deployed to a Flink cluster.
    throw new UnsupportedOperationException("Snapshots are not supported for in-process applications")
  }

  override def restoreApplication(applicationPackageId: String, snapshotPath: String): String = {
    // This class deals with applications running in-process whereas applications can be restored from snapshots only by deploying to a Flink cluster.
    throw new UnsupportedOperationException("Cannot restore applications in-process from snapshot")
  }

  private case class RunningApplicationInstance(instanceId: String,
                                                packageId: String,
                                                instanceDefinition: ApplicationInstance,
                                                streamExecutionEnvironment: StreamExecutionEnvironment,
                                                executionThread: Thread) {
  }

}
