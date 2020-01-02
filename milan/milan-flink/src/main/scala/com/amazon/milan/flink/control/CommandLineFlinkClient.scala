package com.amazon.milan.flink.control

import java.nio.file.Path

import com.amazon.milan.manage.CommandExecutor


class CommandLineFlinkClient(commandExecutor: CommandExecutor) extends FlinkClient {
  override def getRunCommand(jarPath: Path, className: String, args: Seq[String]): String = {
    s"flink run --class $className --detached $jarPath " + args.mkString(" ")
  }

  override def getCancelCommand(flinkApplicationId: String): String = {
    s"flink cancel $flinkApplicationId"
  }

  override def getListRunningCommand(): String = {
    "flink list -r"
  }

  override def getSavepointCommand(flinkApplicationId: String, targetDirectory: String): String = {
    s"flink savepoint $flinkApplicationId $targetDirectory"
  }

  override def getCommandExecutor(): CommandExecutor = {
    this.commandExecutor
  }

  override def getRunFromSavepointCommand(jarPath: Path, savepointPath: String, className: String, args: Seq[String]): String = {
    s"flink run --fromSavepoint $savepointPath --class $className --detached $jarPath " + args.mkString(" ")
  }
}
