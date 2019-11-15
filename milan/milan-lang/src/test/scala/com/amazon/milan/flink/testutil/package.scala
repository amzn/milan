package com.amazon.milan.flink

import java.time.Duration

import com.amazon.milan.flink.application.FlinkApplicationConfiguration
import com.amazon.milan.flink.application.sinks.FlinkSingletonMemorySink
import com.amazon.milan.flink.application.sources.{FlinkListDataSource, SourceFunctionDataSource}
import com.amazon.milan.flink.testutil.SingletonMemorySource
import com.amazon.milan.lang.Stream
import com.amazon.milan.types.LineageRecord
import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.concurrent._
import scala.language.implicitConversions


package object testutil {
  def getTestExecutionEnvironment: StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setBufferTimeout(0)
    env
  }

  implicit def extendApplicationConfiguration(data: FlinkApplicationConfiguration): FlinkApplicationConfigurationExtensions =
    new FlinkApplicationConfigurationExtensions(data)

  implicit def extendFuture[T](future: Future[T]): FutureExtensions[T] =
    new FutureExtensions[T](future)
}


class FlinkApplicationConfigurationExtensions(data: FlinkApplicationConfiguration) {
  def setListSource[T: TypeInformation](stream: Stream[T, _], values: T*): Unit = {
    this.data.setSource(stream, FlinkListDataSource.create[T](values.toList))
  }

  def setMemorySource[T: TypeInformation](stream: Stream[T, _],
                                          stopRunningWhenEmpty: Boolean): SingletonMemorySource[T] = {
    val source = new SingletonMemorySource[T](stopRunningWhenEmpty)
    this.data.setSource(stream, new SourceFunctionDataSource[T](source))
    source
  }

  def setMemorySource[T: TypeInformation](stream: Stream[T, _],
                                          items: Seq[T],
                                          stopRunningWhenEmpty: Boolean): SingletonMemorySource[T] = {
    val source = new SingletonMemorySource[T](items, stopRunningWhenEmpty)
    this.data.setSource(stream, new SourceFunctionDataSource[T](source))
    source
  }

  def addMemorySink[T: TypeDescriptor](stream: Stream[T, _]): FlinkSingletonMemorySink[T] = {
    val sink = FlinkSingletonMemorySink.create[T]
    this.data.addSink(stream, sink)
    sink
  }

  def addMemoryLineageSink(): FlinkSingletonMemorySink[LineageRecord] = {
    val sink = FlinkSingletonMemorySink.create[LineageRecord]
    this.data.addLineageSink(sink)
    sink
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
