package com.amazon.milan.flink.application

import com.amazon.milan.serialization.{GenericTypedJsonDeserializer, SetGenericTypeInfo}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.flink.streaming.api.functions.sink.SinkFunction


@JsonDeserialize(using = classOf[FlinkDataSinkDeserializer])
trait FlinkDataSink[T] extends SetGenericTypeInfo with Serializable {
  def getSinkFunction: SinkFunction[_]

  def getSinkTypeName: String = getClass.getSimpleName
}


class FlinkDataSinkDeserializer extends GenericTypedJsonDeserializer[FlinkDataSink[_]](typeName => s"com.amazon.milan.flink.application.sinks.Flink$typeName")
