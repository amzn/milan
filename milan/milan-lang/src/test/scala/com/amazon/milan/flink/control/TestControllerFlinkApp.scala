package com.amazon.milan.flink.control

import java.io.{InputStream, OutputStream}
import java.nio.file.Path
import java.time.Duration

import com.amazon.milan.control.client.StreamApplicationControllerClient
import com.amazon.milan.control.{ApplicationControllerMessageEnvelope, ApplicationControllerState, StartApplicationMessage}
import com.amazon.milan.flink.application.sinks.SingletonMemorySinkFunction
import com.amazon.milan.flink.testutil.SingletonMemorySource
import com.amazon.milan.manage.PackageRepository
import com.amazon.milan.testing.Concurrent
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.Assert._
import org.junit.Test

import scala.language.reflectiveCalls


@Test
class TestControllerFlinkApp {
  @Test
  def test_ControllerFlinkApp_Execute_WithOneStartApplicationMessageAndFlinkClientThatReturnsNoRunningApplications_OutputsTwoStateMessagesWithApplicationInFirstStateAndSecondStateEmpty(): Unit = {
    val packageRepository = new PackageRepository {
      override def copyToRepository(applicationPackageId: String, packageInputStream: InputStream): Unit = {
      }

      override def copyToStream(applicationPackageId: String, outputStream: OutputStream): Unit = {
      }

      override def copyToFile(applicationPackageId: String, destinationPath: Path): Unit = {
      }
    }

    // Overriding run and list method definitions for the unit test to run
    val flinkClient = new CommandLineFlinkClient(null) {
      override def run(jarPath: Path, className: String, args: String*): String = "flinkId"

      override def listRunning(): Array[String] = Array()

      override def snapshot(flinkApplicationId: String, targetDirectory: String): Option[String] = None
    }

    val controller = new ApplicationController(packageRepository, flinkClient, "/tmp", "", "")
    val controllerMessageHandler = new ApplicationControllerMessageHandler(controller)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // We don't want the source to exit the run method when it goes empty.
    // If it does, the flink app will shut down, possibly before the timer fires that checks the state of
    // the running Milan applications, and we won't get the second state output record.
    val messageSource = SingletonMemorySource.ofItems(false,
      ApplicationControllerMessageEnvelope.wrapMessage(new StartApplicationMessage("instanceId", "packageId")))

    val stateSink = new SingletonMemorySinkFunction[ApplicationControllerState]()
    val diagnosticSink = new SingletonMemorySinkFunction[ApplicationControllerDiagnostic]()

    // Run the flink app, and shut down the message source after we see the desired output on the state output stream.
    // This will cause the flink app to wait to shut down until after we get our desired output.
    assertTrue(
      "Execution timed out.",
      Concurrent.executeAsyncAndWait(
        () => ControllerFlinkApp.execute(controllerMessageHandler, env, messageSource, stateSink, diagnosticSink, Duration.ofMillis(1), "", Int.MaxValue),
        () => stateSink.getRecordCount >= 2,
        () => messageSource.stopWhenEmpty = true,
        Duration.ofSeconds(10)))

    val stateValues = stateSink.getValues.toArray
    assertEquals("Too many state records.", 2, stateValues.length)

    val state1 = stateValues.head
    assertEquals(1, state1.applicationInfo.length)
    assertEquals("instanceId", state1.applicationInfo.head.applicationInstanceId)
    assertEquals("flinkId", state1.applicationInfo.head.controllerApplicationId)

    val state2 = stateValues.last
    assertEquals(0, state2.applicationInfo.length)
  }

  @Test
  def test_ControllerFlinkApp_Execute_WithOneStartApplicationMessage_OutputsDiagnosticRecords(): Unit = {
    val packageRepository = new PackageRepository {
      override def copyToRepository(applicationPackageId: String, packageInputStream: InputStream): Unit = {
      }

      override def copyToStream(applicationPackageId: String, outputStream: OutputStream): Unit = {
      }

      override def copyToFile(applicationPackageId: String, destinationPath: Path): Unit = {
      }
    }

    // Overriding run method definitions for the unit test to run
    val flinkClient = new CommandLineFlinkClient(null) {
      override def run(jarPath: Path, className: String, args: String*): String = "flinkId"
    }

    val controller = new ApplicationController(packageRepository, flinkClient, "/tmp", "", "")
    val controllerMessageHandler = new ApplicationControllerMessageHandler(controller)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val messageSource = SingletonMemorySource.ofItems(ApplicationControllerMessageEnvelope.wrapMessage(new StartApplicationMessage("instanceId", "packageId")))

    val stateSink = new SingletonMemorySinkFunction[ApplicationControllerState]()
    val diagnosticSink = new SingletonMemorySinkFunction[ApplicationControllerDiagnostic]()

    assertTrue(
      Concurrent.executeAndWait(
        () => ControllerFlinkApp.execute(controllerMessageHandler, env, messageSource, stateSink, diagnosticSink, Duration.ofMillis(1), "", Int.MaxValue),
        () => diagnosticSink.getRecordCount > 1,
        Duration.ofSeconds(10)))
  }

  @Test
  def test_ControllerFlinkApp_WithStreamApplicationControllerClient_RespondsToCommands(): Unit = {
    val packageId = "packageId"

    // Set up the controller.
    val packageRepository = new PackageRepository {
      override def copyToRepository(applicationPackageId: String, packageInputStream: InputStream): Unit = {
      }

      override def copyToStream(applicationPackageId: String, outputStream: OutputStream): Unit = {
      }

      override def copyToFile(applicationPackageId: String, destinationPath: Path): Unit = {
        assertEquals(packageId, applicationPackageId)
      }
    }

    // Overriding run and list method definitions for the unit test to run
    val flinkClient = new CommandLineFlinkClient(null) {
      override def run(jarPath: Path, className: String, args: String*): String = "flinkId"

      override def listRunning(): Array[String] = Array("flinkId")

      override def snapshot(flinkApplicationId: String, targetDirectory: String): Option[String] = None
    }

    val controller = new ApplicationController(packageRepository, flinkClient, packageCachePath = "/tmp", "", "")
    val controllerMessageHandler = new ApplicationControllerMessageHandler(controller)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val messageSource = new SingletonMemorySource[ApplicationControllerMessageEnvelope](stopRunningWhenEmpty = false)
    val stateSink = new SingletonMemorySinkFunction[ApplicationControllerState]()
    val diagnosticsSink = new SingletonMemorySinkFunction[ApplicationControllerDiagnostic]()

    // Set up the client that will use the source and sink we created for the controller application.
    val stateGetter = new {
      private var nextIndex = 0

      def getNextState: Option[ApplicationControllerState] = {
        val values = stateSink.getValues
        if (nextIndex >= values.length) {
          None
        }
        else {
          nextIndex = values.length
          Some(values.last)
        }
      }
    }

    val client = new StreamApplicationControllerClient(messageSource.add, () => stateGetter.getNextState)

    // Run the controller.
    val runnable = new Runnable {
      override def run(): Unit =
        ControllerFlinkApp.execute(controllerMessageHandler, env, messageSource, stateSink, diagnosticsSink, Duration.ofMillis(1), "", Int.MaxValue)
    }

    val executionThread = new Thread(runnable)
    executionThread.start()

    client.startApplication("packageId")

    assertTrue(
      Concurrent.wait(
        () => client.listRunningApplications().length == 1,
        Duration.ofSeconds(10)))

    messageSource.stopWhenEmpty = true

    Concurrent.wait(
      () => !executionThread.isAlive,
      Duration.ofSeconds(5))
  }
}
