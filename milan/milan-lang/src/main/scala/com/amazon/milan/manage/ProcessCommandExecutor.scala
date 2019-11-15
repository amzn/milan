package com.amazon.milan.manage

import java.util.concurrent.ConcurrentLinkedQueue

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.sys.process._


object ProcessCommandExecutor {
  def execute(command: String): ExecutionResult = {
    def executor = new ProcessCommandExecutor()

    executor.executeAndWait(command)
  }

  def execute(command: ProcessBuilder): ExecutionResult = {
    def executor = new ProcessCommandExecutor()

    executor.executeAndWait(command)
  }
}


/**
 * A [[CommandExecutor]] implementation that executes commands by starting system processes.
 */
class ProcessCommandExecutor extends CommandExecutor with Serializable {
  @transient private lazy val logger = Logger(LoggerFactory.getLogger(classOf[ProcessCommandExecutor]))

  override def executeAndWait(command: String): ExecutionResult = {
    val processLogger = new MemoryProcessLogger()

    this.logger.info(s"Executing shell command '$command'.")

    val processBuilder = Process.apply(command)
    val proc = processBuilder.run(processLogger)

    this.logger.info("Waiting for process to finish.")
    val exitValue = proc.exitValue()

    ExecutionResult(exitValue, processLogger.outLines.asScala.toArray, processLogger.errLines.asScala.toArray)
  }

  def executeAndWait(command: ProcessBuilder): ExecutionResult = {
    val processLogger = new MemoryProcessLogger()

    this.logger.info(s"Executing shell command '${command.toString}'.")

    val proc = command.run(processLogger)

    this.logger.info("Waiting for process to finish.")
    val exitValue = proc.exitValue()

    ExecutionResult(exitValue, processLogger.outLines.asScala.toArray, processLogger.errLines.asScala.toArray)
  }

  class MemoryProcessLogger extends ProcessLogger {
    val outLines = new ConcurrentLinkedQueue[String]()
    val errLines = new ConcurrentLinkedQueue[String]()

    override def out(s: => String): Unit = this.outLines.add(s)

    override def err(s: => String): Unit = this.errLines.add(s)

    override def buffer[T](f: => T): T = f
  }

}
