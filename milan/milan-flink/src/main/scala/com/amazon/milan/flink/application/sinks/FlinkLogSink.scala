package com.amazon.milan.flink.application.sinks

import com.amazon.milan.flink.application.FlinkDataSink
import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.slf4j.LoggerFactory


@JsonDeserialize
class FlinkLogSink[T] extends FlinkDataSink[T] {
  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
  }

  override def getSinkFunction: SinkFunction[_] = {
    new LogSinkFunction[T]
  }
}


class LogSinkFunction[T] extends SinkFunction[T] {
  @transient private lazy val logger = Logger(LoggerFactory.getLogger(this.getClass))
  @transient private lazy val objectWriter = ScalaObjectMapper.writerWithDefaultPrettyPrinter()

  override def invoke(value: T): Unit = {
    logger.info(objectWriter.writeValueAsString(value))
  }
}
