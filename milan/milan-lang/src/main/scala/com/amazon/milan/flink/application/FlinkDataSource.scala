package com.amazon.milan.flink.application

import com.amazon.milan.serialization.{GenericTypedJsonDeserializer, SetGenericTypeInfo}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment


/**
 * Trait for Flink implementations of Milan data sources.
 * For every Milan [[com.amazon.milan.application.DataSource]] (that is supported in the Flink environment), there will
 * be a corresponding implementation of [[FlinkDataSource]] that can deserialize from the JSON-serialized
 * [[com.amazon.milan.application.DataSource]].
 * These Flink implementations provide the functionality required by the Milan Flink compiler to add the data
 * source to Flink the streaming environment.
 */
@JsonDeserialize(using = classOf[DataSourceDeserializer])
trait FlinkDataSource[T] extends SetGenericTypeInfo with Serializable {
  def addDataSource(env: StreamExecutionEnvironment): DataStreamSource[T]

  def getSourceTypeName: String = getClass.getSimpleName
}


class DataSourceDeserializer extends GenericTypedJsonDeserializer[FlinkDataSource[_]](typeName => s"com.amazon.milan.flink.application.sources.Flink$typeName")
