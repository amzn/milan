package com.amazon.milan.flink.application.sources

import com.amazon.milan.flink.RuntimeEvaluator
import com.amazon.milan.flink.application.FlinkDataSource
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.collection.JavaConverters._


@JsonDeserialize
class FlinkListDataSource[T](val values: List[T],
                             var recordTypeInformation: TypeInformation[T]) extends FlinkDataSource[T] {
  @JsonCreator
  def this(values: List[T]) {
    this(values, null)
  }

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeInformation = RuntimeEvaluator.instance.createTypeInformation[T](genericArgs.head.asInstanceOf[TypeDescriptor[T]])
  }

  override def addDataSource(env: StreamExecutionEnvironment): DataStreamSource[T] = {
    env.fromCollection(this.values.asJavaCollection, this.recordTypeInformation)
  }
}


object FlinkListDataSource {
  def create[T: TypeInformation](values: T*): FlinkListDataSource[T] = {
    new FlinkListDataSource[T](values.toList, implicitly[TypeInformation[T]])
  }

  def create[T: TypeInformation](values: List[T]): FlinkListDataSource[T] = {
    new FlinkListDataSource[T](values, implicitly[TypeInformation[T]])
  }
}
