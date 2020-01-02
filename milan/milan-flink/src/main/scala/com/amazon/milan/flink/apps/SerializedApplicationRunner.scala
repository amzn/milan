package com.amazon.milan.flink.apps

import com.amazon.milan.flink.compiler.FlinkCompiler
import com.typesafe.scalalogging.Logger
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory


object SerializedApplicationRunner {

  private class CmdArgs extends ArgumentsBase {
    @NamedArgument(Name = "application-resource-name", ShortName = "app")
    var applicationResourceName: String = _

    @NamedArgument(Name = "max-parallelism", ShortName = "p", Required = false, DefaultValue = "8")
    var maxParallelism: Int = _

    // Checkpoints are intended to be used by Flink for failure recovery
    @NamedArgument(Name = "checkpoint-directory", ShortName = "cd", Required = false, DefaultValue = "file:///tmp/checkpoints")
    var checkpointDirectory: String = ""
  }

  def main(args: Array[String]): Unit = {
    println("Starting application.")

    val logger = Logger(LoggerFactory.getLogger(this.getClass))

    val params = new CmdArgs()
    params.parse(args)

    logger.info(s"Loading application resource '${params.applicationResourceName}'.")
    val instanceResourceStream = getClass.getResourceAsStream(params.applicationResourceName)

    logger.info("Initializing streaming environment.")
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    logger.info(s"Setting up streaming environment: { maxParallelism=${params.maxParallelism}, parallelism=${params.maxParallelism} }.")
    env.setMaxParallelism(params.maxParallelism)
    env.setStateBackend(new RocksDBStateBackend(params.checkpointDirectory, true).asInstanceOf[StateBackend])

    logger.info("Compiling application.")
    FlinkCompiler.defaultCompiler.compileFromInstanceJson(instanceResourceStream, env)

    logger.info("Executing streaming environment.")
    env.execute()
  }
}
