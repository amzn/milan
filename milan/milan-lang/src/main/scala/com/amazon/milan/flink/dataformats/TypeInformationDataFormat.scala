package com.amazon.milan.flink.dataformats

import java.io.ByteArrayInputStream

import com.amazon.milan.dataformats.DataFormat
import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.core.memory.DataInputViewStreamWrapper


/**
 * A [[DataFormat]] that uses [[TypeInformation]] to deserialize objects.
 *
 * @param typeInfo A [[TypeInformation]] that can produce a deserializer for the objects.
 * @tparam T The type of objects.
 */
class TypeInformationDataFormat[T](typeInfo: TypeInformation[T]) extends DataFormat[T] {
  @transient private lazy val serializer = this.createSerializer()

  override def getGenericArguments: List[TypeDescriptor[_]] = {
    // This class is not intended to be serialized by GenericTypedJsonSerializer, so this should not be called.
    throw new UnsupportedOperationException()
  }

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    // This class is not intended to be deserialized by GenericTypedJsonDeserializer, so this should not be called.
    throw new UnsupportedOperationException()
  }

  override def readValue(bytes: Array[Byte], offset: Int, length: Int): T = {
    val input = new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes, offset, length))
    this.serializer.deserialize(input)
  }

  private def createSerializer(): TypeSerializer[T] = {
    val config = new ExecutionConfig()
    this.typeInfo.createSerializer(config)
  }
}
