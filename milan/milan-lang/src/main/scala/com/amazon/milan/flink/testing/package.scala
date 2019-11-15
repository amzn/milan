package com.amazon.milan.flink

import java.time.Duration
import java.util.concurrent.TimeoutException

import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.flink.compiler.FlinkCompiler
import com.amazon.milan.lang.StreamGraph
import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.testing.Concurrent
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions


package object testing {
  implicit def extendStreamExecutionEnvironment(env: StreamExecutionEnvironment): StreamExecutionEnvironmentExtensions =
    new StreamExecutionEnvironmentExtensions(env)

  /**
   * Serializes a graph and application configuration as an [[ApplicationInstance]] and invokes the Flink compiler
   * using the serialized instance.
   *
   * @param graph             A stream graph.
   * @param config            An application configuration.
   * @param targetEnvironment A Flink streaming environment to be used as the compilation target.
   */
  def compileFromSerialized(graph: StreamGraph,
                            config: ApplicationConfiguration,
                            targetEnvironment: StreamExecutionEnvironment): Unit = {
    val application = new Application(graph)
    val instance = new ApplicationInstance(application, config)
    val json = ScalaObjectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(instance)
    FlinkCompiler.defaultCompiler.compileFromInstanceJson(json, targetEnvironment)
  }
}


class StreamExecutionEnvironmentExtensions(env: StreamExecutionEnvironment) {
  def executeThenWaitFor(predicate: () => Boolean, secondsToWait: Int): Unit = {
    if (!Concurrent.executeAndWait(
      () => env.execute(),
      predicate,
      Duration.ofSeconds(secondsToWait))) {
      throw new TimeoutException("Timed out waiting for stop condition.")
    }
  }

  def executeUntilAsync(predicate: () => Boolean, secondsToWait: Int): Future[Unit] = {
    val result =
      Concurrent.executeAsync(
        () => env.execute(),
        predicate,
        Duration.ofSeconds(secondsToWait))

    result.transform(
      success =>
        if (!success) {
          throw new TimeoutException("Timed out waiting for stop condition.")
        },
      ex => throw ex)(ExecutionContext.global)
  }

  def executeAtMost(maxSeconds: Int): Unit = {
    if (!Concurrent.executeUntil(
      () => env.execute(),
      () => true,
      Duration.ofSeconds(maxSeconds))) {
      throw new TimeoutException("Timed out waiting for stop condition.")
    }
  }
}
