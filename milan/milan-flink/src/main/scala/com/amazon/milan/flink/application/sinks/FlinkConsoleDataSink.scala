package com.amazon.milan.flink.application.sinks

import com.amazon.milan.flink.application.FlinkDataSink
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.flink.streaming.api.functions.sink.SinkFunction


@JsonDeserialize
class FlinkConsoleDataSink[T] extends FlinkDataSink[T] {
  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
  }

  override def getSinkFunction: SinkFunction[T] = new LogSinkFunction[T]
}
