package com.amazon.milan.flink.runtime

import java.net.URLClassLoader

import com.amazon.milan.cmd.{ArgumentsBase, NamedArgument}
import com.typesafe.scalalogging.Logger
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.slf4j.LoggerFactory


class MilanApplicationCmdArgs extends ArgumentsBase {
  @NamedArgument(Name = "max-parallelism", ShortName = "mp", Required = false, DefaultValue = "0")
  var maxParallelism: Int = 0

  @NamedArgument(Name = "list-classpath", ShortName = "listcp", Required = false, DefaultValue = "false")
  var listClassPath: Boolean = false

  @NamedArgument(Name = "state-backend", ShortName = "sbe", Required = false, DefaultValue = "default")
  var stateBackend: String = _

  @NamedArgument(Name = "checkpoint-directory", ShortName = "cd", Required = false, DefaultValue = "file:///tmp/checkpoints")
  var checkpointDirectory: String = _

  @NamedArgument(Name = "checkpoint-interval", ShortName = "ci", Required = false, DefaultValue = "30")
  var checkpointIntervalSeconds: Int = 30
}


abstract class MilanApplicationBase {
  private val logger = Logger(LoggerFactory.getLogger(getClass))

  def buildFlinkApplication(env: StreamExecutionEnvironment): Unit

  def hasCycles: Boolean

  def execute(args: Array[String]): Unit = {
    val cmdArgs = new MilanApplicationCmdArgs
    cmdArgs.parse(args, allowUnknownArguments = true)

    if (cmdArgs.listClassPath) {
      this.listClassPathUrls()
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    if (cmdArgs.maxParallelism > 0) {
      env.setMaxParallelism(cmdArgs.maxParallelism)
    }

    cmdArgs.stateBackend match {
      case "rocksdb" =>
        this.logger.info("Using RocksDB state back-end.")
        env.setStateBackend(new RocksDBStateBackend(cmdArgs.checkpointDirectory, true).asInstanceOf[StateBackend])

      case "default" => ()

      case _ =>
        throw new IllegalArgumentException("state-backend must be one of: rocksdb, default")
    }

    if (!this.hasCycles) {
      env.enableCheckpointing(cmdArgs.checkpointIntervalSeconds * 1000, CheckpointingMode.AT_LEAST_ONCE)
    }

    this.buildFlinkApplication(env)
    env.execute()
  }

  private def listClassPathUrls(): Unit = {
    ClassLoader.getSystemClassLoader match {
      case urlClassLoader: URLClassLoader =>
        urlClassLoader.getURLs.foreach(url => logger.info(s"ClassPath: $url"))

      case _ =>
        this.logger.error(s"Can't list ClassPath URLs for ClassLoader of type '${ClassLoader.getSystemClassLoader.getClass.getName}'.")
    }
  }
}
