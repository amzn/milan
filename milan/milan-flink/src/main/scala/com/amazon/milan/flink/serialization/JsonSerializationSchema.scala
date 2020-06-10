package com.amazon.milan.flink.serialization

import java.nio.ByteBuffer

import com.amazon.milan.serialization.MilanObjectMapper
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisSerializationSchema


object JsonSerializationSchema {
  private val objectMapper = new MilanObjectMapper()
}


/**
 * A [[KinesisSerializationSchema]] that handles serializing objects as JSON.
 *
 * @tparam T The type of objects being serialized.
 */
class JsonSerializationSchema[T]()
  extends KinesisSerializationSchema[T]
    with Serializable {

  override def serialize(value: T): ByteBuffer = {
    ByteBuffer.wrap(JsonSerializationSchema.objectMapper.writeValueAsBytes(value))
  }

  override def getTargetStream(value: T): String = {
    // The target stream is configured separately by the caller and does not depend on the value being serialized.
    null
  }
}
