package com.amazon.milan.flink.control

import java.nio.file.Path
import java.util.regex.Pattern

import com.amazon.milan.manage.CommandExecutor
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


abstract class FlinkClient extends Serializable {
  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  // Commands to be defined in the derived classes
  def getRunCommand(jarPath: Path, className: String, args: Seq[String]): String

  def getCancelCommand(flinkApplicationId: String): String

  def getListRunningCommand(): String

  def getSavepointCommand(flinkApplicationId: String, targetDirectory: String): String

  def getRunFromSavepointCommand(jarPath: Path, savepointPath: String, className: String, args: Seq[String]): String

  def getCommandExecutor(): CommandExecutor

  /**
   * Starts a flink application in detached mode.
   *
   * @param jarPath   The path to the jar containing the application.
   * @param className The name of the application class in the jar.
   * @param args      Additional arguments to the "flink run" command.
   * @return The flink application ID for the started application.
   */
  def run(jarPath: Path, className: String, args: String*): String = {
    val shellCommand = getRunCommand(jarPath, className, args)
    this.startFlinkAppAndReturnAppId(shellCommand)
  }

  /**
   * Starts a flink application in detached mode with state restored from given savepoint.
   *
   * @param jarPath       The path to the jar containing the application.
   * @param savepointPath Absolute path to either the savepoint’s directory or the _metadata file.
   * @param className     The name of the application class in the jar.
   * @param args          Additional arguments to the "flink run" command.
   * @return The flink application ID of the started application.
   */
  def runFromSavepoint(jarPath: Path, savepointPath: String, className: String, args: String*): String = {
    val shellCommand = getRunFromSavepointCommand(jarPath, savepointPath, className, args)
    this.startFlinkAppAndReturnAppId(shellCommand)
  }

  /**
   * Starts a flink application using the provided runCommand.
   * Note: This method is private to FlinkClient and not exposed externally.
   *
   * @param runCommand The "flink run" shell command to be executed to start a flink application.
   * @return The flink application ID of the started application.
   */
  private def startFlinkAppAndReturnAppId(runCommand: String): String = {
    val output = getCommandExecutor().executeAndWait(runCommand)

    if (output.exitCode != 0) {
      throw new FlinkCommandException(output.exitCode, output.getFullErrorOutput)
    }

    val lastLine = output.standardOutput.last
    val applicationId = lastLine.substring(lastLine.lastIndexOf(' ') + 1)

    applicationId
  }

  /**
   * Cancels a running flink application.
   *
   * @param flinkApplicationId The ID of the flink application to cancel.
   */
  def cancel(flinkApplicationId: String): Unit = {
    this.logger.info(s"Stopping flink application '$flinkApplicationId'.")
    val shellCommand = getCancelCommand(flinkApplicationId)
    val output = getCommandExecutor().executeAndWait(shellCommand)

    if (output.exitCode != 0) {
      throw new FlinkCommandException(output.exitCode, output.errorOutput.mkString("\n"))
    }
  }

  /**
   * Gets a list of the flink application IDs of running applications.
   *
   * @return A list of application IDs.
   */
  def listRunning(): Array[String] = {
    val output = getCommandExecutor().executeAndWait(getListRunningCommand())

    if (output.exitCode != 0) {
      throw new FlinkCommandException(output.exitCode, output.errorOutput.mkString("\n"))
    }

    val pattern = Pattern.compile(".* \\: ([^\\ ]*) \\: .*")

    output.standardOutput.toSeq
      .dropWhile(line => !line.contains("Running/Restarting Jobs"))
      .drop(1)
      .takeWhile(line => !line.startsWith("-"))
      .map(line => pattern.matcher(line))
      .filter(_.matches())
      .map(_.group(1))
      .toArray
  }

  /**
   * Creates snapshot for a flink application.
   *
   * @param flinkApplicationId The ID of the flink application for which snapshot should be created.
   * @param targetDirectory    Directory where the snapshot should be stored.
   *                           The location has to be accessible by both the JobManager(s) and TaskManager(s).
   *                           Can be an S3 path for example.
   *
   */
  def snapshot(flinkApplicationId: String, targetDirectory: String): Option[String] = {
    val shellCommand = getSavepointCommand(flinkApplicationId, targetDirectory)
    this.logger.info(s"Executing savepoint command $shellCommand")
    val output = getCommandExecutor().executeAndWait(shellCommand)

    if (output.exitCode != 0) {
      throw new FlinkCommandException(output.exitCode, output.getFullErrorOutput)
    }
    this.logger.debug("Savepoint command output: ", output.getFullStandardOutput)

    // Parse savepoint output path
    val stdOutLinePrefix = "Savepoint completed. Path:"
    for (line <- output.standardOutput) {
      if (line.startsWith(stdOutLinePrefix))
        return Some(line.replace(stdOutLinePrefix, "").trim())
    }
    None // return None if cannot find savepoint path in command output
  }
}


class FlinkCommandException(val exitCode: Int, val errorOutput: String)
  extends Exception(s"The process completed with exit code $exitCode.") {
}
