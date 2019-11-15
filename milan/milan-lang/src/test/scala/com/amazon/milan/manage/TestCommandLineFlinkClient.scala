package com.amazon.milan.manage

import java.nio.file.Paths

import com.amazon.milan.flink.control.CommandLineFlinkClient
import org.hamcrest.CoreMatchers._
import org.junit.Assert._
import org.junit.Test


@Test
class TestCommandLineFlinkClient {
  @Test
  def test_CommandLineFlinkClient_Run_CallsCommandExecutorWithRunCommandAndParsesJobId(): Unit = {
    val commandExecutor = new CommandExecutor {
      override def executeAndWait(command: String): ExecutionResult = {
        val lines =
          if (command.startsWith("flink run")) {
            Array("Starting execution of program",
              "Job has been submitted with JobID 2b7836a3a6cf7380703c2d362ac258cf")
          }
          else {
            return ExecutionResult(-1, Array(), Array())
          }

        ExecutionResult(0, lines, Array())
      }
    }

    val client = new CommandLineFlinkClient(commandExecutor)

    val flinkAppId = client.run(Paths.get("/tmp"), "className")
    assertEquals("2b7836a3a6cf7380703c2d362ac258cf", flinkAppId)
  }

  @Test
  def test_CommandLineFlinkClient_ListRunning_CallsCommandExecutorWithListCommandAndParsesRunningJobId(): Unit = {
    val commandExecutor = new CommandExecutor {
      override def executeAndWait(command: String): ExecutionResult = {
        val lines =
          if (command.startsWith("flink list")) {
            Array("Waiting for response...",
              "------------------ Running/Restarting Jobs -------------------",
              "22.02.2019 10:04:34 : 2b7836a3a6cf7380703c2d362ac258cf : Flink Streaming Job (RUNNING)",
              "--------------------------------------------------------------")
          }
          else {
            return ExecutionResult(-1, Array(), Array())
          }

        ExecutionResult(0, lines, Array())
      }
    }

    val client = new CommandLineFlinkClient(commandExecutor)
    val runningAppIds = client.listRunning()
    assertThat(runningAppIds, is(Array("2b7836a3a6cf7380703c2d362ac258cf")))

  }

  @Test
  def test_CommandLineFlinkClient_Cancel_CallsCommandExecutorWithCancelCommand(): Unit = {
    var cancelCalled: Boolean = false

    val commandExecutor = new CommandExecutor {
      override def executeAndWait(command: String): ExecutionResult = {
        if (command.startsWith("flink cancel")) {
          cancelCalled = true
          ExecutionResult(0, Array(), Array())
        }
        else {
          ExecutionResult(-1, Array(), Array())
        }
      }
    }

    val client = new CommandLineFlinkClient(commandExecutor)
    client.cancel("appId")

    assertTrue(cancelCalled)
  }

  @Test
  def test_CommandLineFlinkClient_SnapshotApplication_CallsCommandExecutorAndParsesSnapshotPath(): Unit = {
    val commandExecutor = new CommandExecutor {
      override def executeAndWait(command: String): ExecutionResult = {
        val lines =
          if (command.startsWith("flink savepoint")) {
            Array("Waiting for response...",
              "Savepoint completed. Path: file:/tmp/snapshots/savepoint-bdc79f-eeee8dc79a57",
              "You can resume your program from this savepoint with the run command.")
          }
          else {
            return ExecutionResult(-1, Array(), Array())
          }

        ExecutionResult(0, lines, Array())
      }
    }

    val client = new CommandLineFlinkClient(commandExecutor)
    val snapshotPath = client.snapshot("appId", "/tmp/snapshots/")
    assertEquals(Some("file:/tmp/snapshots/savepoint-bdc79f-eeee8dc79a57"), snapshotPath)
  }

  @Test
  def test_CommandLineFlinkClient_RunFromSavepoint_CallsCommandExecutorAndParsesJobId(): Unit = {
    val commandExecutor = new CommandExecutor {
      override def executeAndWait(command: String): ExecutionResult = {
        val lines =
          if (command.startsWith("flink run --fromSavepoint")) {
            Array("Starting execution of program",
              "Starting application.",
              "Job has been submitted with JobID 654d5cd3e97c5dff45ae4cd0d8465d7b")
          }
          else {
            return ExecutionResult(-1, Array(), Array())
          }

        ExecutionResult(0, lines, Array())
      }
    }

    val client = new CommandLineFlinkClient(commandExecutor)

    val flinkAppId = client.runFromSavepoint(Paths.get("/tmp"), "savepointPath", "className")
    assertEquals("654d5cd3e97c5dff45ae4cd0d8465d7b", flinkAppId)
  }
}
