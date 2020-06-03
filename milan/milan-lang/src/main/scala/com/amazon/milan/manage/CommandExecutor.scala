package com.amazon.milan.manage

import java.time.Duration


case class ExecutionResult(exitCode: Int, standardOutput: Array[String], errorOutput: Array[String]) {
  def getFullStandardOutput: String = this.standardOutput.mkString("\n")

  def getFullErrorOutput: String = this.errorOutput.mkString("\n")

  def isSuccess: Boolean = this.exitCode == 0
}


trait CommandExecutor {
  def executeAndWait(command: String, maxRuntime: Duration): ExecutionResult

  def executeAndWait(command: String): ExecutionResult = this.executeAndWait(command, Duration.ZERO)
}
