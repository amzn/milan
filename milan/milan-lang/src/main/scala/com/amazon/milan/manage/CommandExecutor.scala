package com.amazon.milan.manage


case class ExecutionResult(exitCode: Int, standardOutput: Array[String], errorOutput: Array[String]) {
  def getFullStandardOutput: String = this.standardOutput.mkString("\n")

  def getFullErrorOutput: String = this.errorOutput.mkString("\n")
}


trait CommandExecutor {
  def executeAndWait(command: String): ExecutionResult
}
