package com.amazon.milan.flink.serialization

import com.amazon.milan.serialization.ScalaObjectMapper
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema

import scala.reflect.{ClassTag, classTag}


object JsonDeserializationSchema {
  private val objectMapper = new ScalaObjectMapper()
}


/**
 * A [[KinesisDeserializationSchema]] that handles deserializing the record data as a JSON object.
 *
 * @tparam T The type of object to deserialize from the JSON record data.
 */
class JsonDeserializationSchema[T: ClassTag] extends KinesisDeserializationSchema[T] with Serializable {
  override def deserialize(bytes: Array[Byte],
                           partitionKey: String,
                           seqNum: String,
                           approxArrivalTimestamp: Long,
                           stream: String,
                           shardId: String): T = {
    JsonDeserializationSchema.objectMapper.readValue[T](bytes, classTag[T].runtimeClass.asInstanceOf[Class[T]])
  }

  override def getProducedType: TypeInformation[T] = {
    TypeExtractor.getForClass(classTag[T].runtimeClass.asInstanceOf[Class[T]])
  }
}
