package com.amazon.milan.flink.control

import java.nio.file.Path

import com.amazon.milan.manage.CommandExecutor


class CommandLineFlinkYarnClient(commandExecutor: CommandExecutor, yarnApplicationId: String) extends FlinkClient {
  override def getRunCommand(jarPath: Path, className: String, args: Seq[String]): String = {
    s"flink run -yid ${this.yarnApplicationId} --class $className --detached $jarPath " + args.mkString(" ")
  }

  override def getCancelCommand(flinkApplicationId: String): String = {
    s"flink cancel -yid ${this.yarnApplicationId} $flinkApplicationId"
  }

  override def getListRunningCommand(): String = {
    s"flink list -r -yid ${this.yarnApplicationId}"
  }

  override def getSavepointCommand(flinkApplicationId: String, targetDirectory: String): String = {
    s"flink savepoint $flinkApplicationId $targetDirectory -yid ${this.yarnApplicationId}"
  }

  override def getCommandExecutor(): CommandExecutor = {
    this.commandExecutor
  }

  override def getRunFromSavepointCommand(jarPath: Path, savepointPath: String, className: String, args: Seq[String]): String = {
    s"flink run -yid ${this.yarnApplicationId} --fromSavepoint $savepointPath --class $className --detached $jarPath " + args.mkString(" ")
  }
}
