package com.amazon.milan.flink

import java.time.Duration
import java.util.concurrent.TimeoutException

import com.amazon.milan.Id
import com.amazon.milan.flink.application.FlinkApplicationConfiguration
import com.amazon.milan.flink.application.sinks.FlinkSingletonMemorySink
import com.amazon.milan.flink.application.sources.{FlinkListDataSource, SourceFunctionDataSource}
import com.amazon.milan.flink.testing.SingletonMemorySource
import com.amazon.milan.lang.Stream
import com.amazon.milan.testing.Concurrent
import com.amazon.milan.types.LineageRecord
import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.concurrent.{Await, ExecutionContext, Future, blocking}
import scala.language.implicitConversions


package object testing {
  implicit def extendStreamExecutionEnvironment(env: StreamExecutionEnvironment): StreamExecutionEnvironmentExtensions =
    new StreamExecutionEnvironmentExtensions(env)

  implicit def extendApplicationConfiguration(data: FlinkApplicationConfiguration): FlinkApplicationConfigurationExtensions =
    new FlinkApplicationConfigurationExtensions(data)

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


class FlinkApplicationConfigurationExtensions(data: FlinkApplicationConfiguration) {
  def setListSource[T: TypeInformation](stream: Stream[T], values: T*): Unit = {
    this.data.setSource(stream, FlinkListDataSource.create[T](values.toList))
  }

  def setMemorySource[T: TypeInformation](stream: Stream[T],
                                          stopRunningWhenEmpty: Boolean,
                                          sourceId: String): SingletonMemorySource[T] =
    this.setMemorySource(stream, List(), stopRunningWhenEmpty, Some(sourceId))

  def setMemorySource[T: TypeInformation](stream: Stream[T],
                                          stopRunningWhenEmpty: Boolean): SingletonMemorySource[T] =
    this.setMemorySource(stream, List(), stopRunningWhenEmpty, None)

  def setMemorySource[T: TypeInformation](stream: Stream[T],
                                          items: Seq[T],
                                          stopRunningWhenEmpty: Boolean,
                                          sourceId: String): SingletonMemorySource[T] =
    this.setMemorySource(stream, items, stopRunningWhenEmpty, Some(sourceId))

  def setMemorySource[T: TypeInformation](stream: Stream[T],
                                          items: Seq[T],
                                          stopRunningWhenEmpty: Boolean): SingletonMemorySource[T] =
    this.setMemorySource(stream, items, stopRunningWhenEmpty, None)

  def addMemorySink[T: TypeDescriptor](stream: Stream[T], sinkid: String): FlinkSingletonMemorySink[T] = {
    val sink = FlinkSingletonMemorySink.create[T](sinkid)
    this.data.addSink(stream, sink)
    sink
  }

  def addMemorySink[T: TypeDescriptor](stream: Stream[T]): FlinkSingletonMemorySink[T] =
    this.addMemorySink(stream, Id.newId())

  def addMemoryLineageSink(): FlinkSingletonMemorySink[LineageRecord] = {
    val sink = FlinkSingletonMemorySink.create[LineageRecord]
    this.data.addLineageSink(sink)
    sink
  }

  private def setMemorySource[T: TypeInformation](stream: Stream[T],
                                                  items: Seq[T],
                                                  stopRunningWhenEmpty: Boolean,
                                                  sourceId: Option[String]): SingletonMemorySource[T] = {
    val source = new SingletonMemorySource[T](items, stopRunningWhenEmpty, sourceId)
    this.data.setSource(stream, new SourceFunctionDataSource[T](source))
    source
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
