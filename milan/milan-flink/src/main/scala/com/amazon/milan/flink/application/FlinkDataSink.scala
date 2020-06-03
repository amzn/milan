package com.amazon.milan.flink.application

import com.amazon.milan.flink.MilanFlinkConfiguration
import com.amazon.milan.serialization.{GenericTypedJsonDeserializer, SetGenericTypeInfo}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.flink.streaming.api.functions.sink.SinkFunction


@JsonDeserialize(using = classOf[FlinkDataSinkDeserializer])
trait FlinkDataSink[T] extends SetGenericTypeInfo with Serializable {
  def getSinkFunction: SinkFunction[T]

  def getSinkTypeName: String = getClass.getSimpleName

  def getMaxParallelism: Option[Int] = None
}


class FlinkDataSinkDeserializer extends GenericTypedJsonDeserializer[FlinkDataSink[_]](typeName => s"com.amazon.milan.flink.application.sinks.Flink$typeName") {
  override protected def getTypeClass(typeName: String): Class[_ <: FlinkDataSink[_]] = {
    MilanFlinkConfiguration.instance.getDataSinkClass(typeName) match {
      case Some(cls) =>
        cls

      case None =>
        super.getTypeClass(typeName)
    }
  }
}
