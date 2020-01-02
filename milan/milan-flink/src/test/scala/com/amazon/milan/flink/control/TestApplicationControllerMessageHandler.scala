package com.amazon.milan.control

import java.io.{InputStream, OutputStream}
import java.nio.file.Path

import com.amazon.milan.flink.control.{ApplicationController, ApplicationControllerMessageHandler, CommandLineFlinkClient}
import com.amazon.milan.manage.PackageRepository
import com.amazon.milan.serialization.ScalaObjectMapper
import org.junit.Assert._
import org.junit.Test


@Test
class TestApplicationControllerMessageHandler {
  @Test
  def test_ApplicationControllerMessageHandler_ProcessMessage_ThenSaveState_ReturnsOneApplicationWithExpectedIds(): Unit = {
    val mapper = new ScalaObjectMapper()
    val json = mapper.writeValueAsString(new StartApplicationMessage("instanceId", "packageId"))

    val packageRepository = new PackageRepository {
      override def copyToRepository(applicationPackageId: String, packageInputStream: InputStream): Unit = {
      }

      override def copyToStream(applicationPackageId: String, outputStream: OutputStream): Unit = {
      }

      override def copyToFile(applicationPackageId: String, destinationPath: Path): Unit = {
      }
    }

    val flinkClient = new CommandLineFlinkClient(null) {
      // Overriding run method definition for the unit test to run
      override def run(jarPath: Path, className: String, args: String*): String = "flinkId"
    }

    val controller = new ApplicationController(packageRepository, flinkClient, "/tmp", "", "")
    val target = new ApplicationControllerMessageHandler(controller)
    target.processMessage(json)

    val state = target.saveState(DEFAULT_CONTROLLER_ID)

    assertEquals(1, state.applicationInfo.length)
    assertEquals("instanceId", state.applicationInfo(0).applicationInstanceId)
    assertEquals("flinkId", state.applicationInfo(0).controllerApplicationId)
  }

  @Test
  def test_ApplicationControllerMessageHandler_ProcessMessage_ThenCheckForChanges_ThenSaveState_ReturnsNoApplicationsWhenApplicationReportedStopped(): Unit = {
    val mapper = new ScalaObjectMapper()
    val json = mapper.writeValueAsString(new StartApplicationMessage("instanceId", "packageId"))

    val packageRepository = new PackageRepository {
      override def copyToRepository(applicationPackageId: String, packageInputStream: InputStream): Unit = {
      }

      override def copyToStream(applicationPackageId: String, outputStream: OutputStream): Unit = {
      }

      override def copyToFile(applicationPackageId: String, destinationPath: Path): Unit = {
      }
    }

    val flinkClient = new CommandLineFlinkClient(null) {
      // Overriding run and listRunning method definitions for the unit test to run
      override def run(jarPath: Path, className: String, args: String*): String = "flinkId"

      override def listRunning(): Array[String] = Array()
    }

    val controller = new ApplicationController(packageRepository, flinkClient, "/tmp", "", "")
    val target = new ApplicationControllerMessageHandler(controller)

    target.processMessage(json)

    val state1 = target.saveState(DEFAULT_CONTROLLER_ID)
    assertEquals(1, state1.applicationInfo.length)

    assertTrue(target.checkForChanges())
    val state2 = target.saveState(DEFAULT_CONTROLLER_ID)
    assertEquals(0, state2.applicationInfo.length)
  }
}
