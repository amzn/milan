package com.amazon.milan.compiler.flink

import java.time.Duration
import java.util.concurrent.TimeoutException

import com.amazon.milan.testing.Concurrent
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.concurrent.{Await, ExecutionContext, Future, blocking}
import scala.language.implicitConversions


package object testing {
  implicit def extendStreamExecutionEnvironment(env: StreamExecutionEnvironment): StreamExecutionEnvironmentExtensions =
    new StreamExecutionEnvironmentExtensions(env)

  implicit def extendFuture[T](future: Future[T]): FutureExtensions[T] =
    new FutureExtensions[T](future)

  implicit class DurationExtensions(d: Duration) {
    def toConcurrent: scala.concurrent.duration.Duration =
      scala.concurrent.duration.Duration(this.d.toMillis, scala.concurrent.duration.MILLISECONDS)
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

  def executeAsync(maxSeconds: Int): Future[Boolean] = {
    Concurrent.executeAsync(() => env.execute(), () => true, Duration.ofSeconds(maxSeconds))
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


class FutureExtensions[T](future: Future[T]) {
  def thenWaitFor(duration: Duration)(implicit context: ExecutionContext): Future[T] = {
    Future {
      blocking {
        val result = Await.result(this.future, scala.concurrent.duration.Duration.Inf)
        Thread.sleep(duration.toMillis)
        result
      }
    }
  }
}
