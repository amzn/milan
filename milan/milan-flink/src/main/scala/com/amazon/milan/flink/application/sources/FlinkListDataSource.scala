package com.amazon.milan.flink.application.sources

import com.amazon.milan.flink.RuntimeEvaluator
import com.amazon.milan.flink.application.FlinkDataSource
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


@JsonDeserialize
class FlinkListDataSource[T](val values: List[T],
                             val runForever: Boolean,
                             var recordTypeInformation: TypeInformation[T]) extends FlinkDataSource[T] {
  @JsonCreator
  def this(values: List[T], runForever: Boolean) {
    this(values, runForever, null)
  }

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeInformation = RuntimeEvaluator.instance.createTypeInformation[T](genericArgs.head.asInstanceOf[TypeDescriptor[T]])
  }

  override def addDataSource(env: StreamExecutionEnvironment): DataStreamSource[T] = {
    if (this.runForever) {
      // If we don't want the source to terminate after the elements run out then we need to use a custom source
      // function rather than env.fromCollection. In order to not cause duplicate records to be sent from multiple
      // copies of the source function we set the parallelism to 1.
      val source = new ListSourceFunction[T](this.values, this.runForever)
      env.addSource(source, this.recordTypeInformation).setParallelism(1)
    }
    else {
      env.fromCollection(this.values.asJavaCollection, this.recordTypeInformation)
    }
  }
}


class ListSourceFunction[T](values: List[T], runForever: Boolean) extends SourceFunction[T] {
  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  private var running = false

  override def run(sourceContext: SourceFunction.SourceContext[T]): Unit = {
    this.running = true

    values.foreach(sourceContext.collect)

    if (this.runForever) {
      this.logger.info(s"ListSourceFunction items exhausted, awaiting cancellation.")

      while (this.running) {
        Thread.sleep(100)
      }
    }
  }

  override def cancel(): Unit = {
    this.logger.info(s"ListSourceFunction cancelled.")
    this.running = false
  }
}


object FlinkListDataSource {
  def create[T: TypeInformation](values: T*): FlinkListDataSource[T] = {
    new FlinkListDataSource[T](values.toList, false, implicitly[TypeInformation[T]])
  }

  def create[T: TypeInformation](values: List[T]): FlinkListDataSource[T] = {
    new FlinkListDataSource[T](values, false, implicitly[TypeInformation[T]])
  }
}
