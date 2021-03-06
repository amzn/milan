package com.amazon.milan.compiler.flink.testing

import java.nio.file.Paths
import java.time.{Duration, Instant}
import java.util.concurrent.ConcurrentLinkedQueue

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Future, blocking}
import scala.sys.process._


case class AsyncExecutionResult(process: Process, result: Future[ProcessExecutionResult])


case class ProcessExecutionResult(exitCode: Int, standardOutput: Array[String], errorOutput: Array[String]) {
  def getFullStandardOutput: String = this.standardOutput.mkString("\n")

  def getFullErrorOutput: String = this.errorOutput.mkString("\n")

  def isSuccess: Boolean = this.exitCode == 0
}


class ProcessCommandExecutor(workingDirectory: Option[String] = None) {
  @transient private lazy val logger = Logger(LoggerFactory.getLogger(classOf[ProcessCommandExecutor]))

  def execute(command: String, maxRuntime: Duration = Duration.ZERO)
             (implicit executionContext: ExecutionContext): AsyncExecutionResult = {
    val processBuilder =
      workingDirectory match {
        case Some(dir) => Process(command, Paths.get(dir).toFile)
        case None => Process(command)
      }

    this.execute(processBuilder, maxRuntime)
  }

  def execute(processBuilder: ProcessBuilder, maxRuntime: Duration): AsyncExecutionResult = {
    this.logger.info(s"Executing shell command '${processBuilder.toString}'.")

    val processLogger = new LogProcessLogger()
    val process = processBuilder.run(processLogger)

    val result = Future {
      blocking {
        if (!this.waitForProcess(process, maxRuntime)) {
          this.logger.error("Process did not finish within the specified runtime.")
          process.destroy()
          processLogger.err("Process runtime exceeded.")

          ProcessExecutionResult(-1, processLogger.outLines.asScala.toArray, processLogger.errLines.asScala.toArray)
        }
        else {
          val exitValue = process.exitValue()
          this.logger.info(s"Process finished with exit code $exitValue.")

          ProcessExecutionResult(exitValue, processLogger.outLines.asScala.toArray, processLogger.errLines.asScala.toArray)
        }
      }
    }

    AsyncExecutionResult(process, result)

  }

  def executeAndWait(command: String, maxRuntime: Duration = Duration.ZERO): ProcessExecutionResult = {
    val futureResult = this.execute(command, maxRuntime).result
    Await.result(futureResult, scala.concurrent.duration.Duration.Zero)
  }

  def executeAndWait(processBuilder: ProcessBuilder, maxRuntime: Duration): ProcessExecutionResult = {
    val futureResult = this.execute(processBuilder, maxRuntime).result
    Await.result(futureResult, scala.concurrent.duration.Duration.Zero)
  }

  /**
   * Returns whether a process finishes executing in a specified amount of time.
   * Blocks until the process is finished or the specified time is elapsed.
   */
  private def waitForProcess(process: Process, maxRunTime: Duration): Boolean = {
    if (maxRunTime.isZero) {
      process.exitValue()
      true
    }
    else {
      val endTime = Instant.now.plus(maxRunTime)

      while (Instant.now.isBefore(endTime) && process.isAlive()) {
        Thread.sleep(1)
      }

      !process.isAlive()
    }
  }


  class LogProcessLogger extends ProcessLogger {
    val outLines = new ConcurrentLinkedQueue[String]()
    val errLines = new ConcurrentLinkedQueue[String]()

    override def out(s: => String): Unit = {
      val message = s
      this.outLines.add(message)
      logger.info(message)
    }

    override def err(s: => String): Unit = {
      val message = s
      this.errLines.add(message)
      logger.error(message)
    }

    override def buffer[T](f: => T): T = f
  }

}
