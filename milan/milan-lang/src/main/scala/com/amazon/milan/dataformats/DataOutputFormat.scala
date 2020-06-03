package com.amazon.milan.dataformats

import java.io.{ByteArrayOutputStream, OutputStream}

import com.amazon.milan.serialization.{GenericTypeInfoProvider, GenericTypedJsonDeserializer, GenericTypedJsonSerializer, SetGenericTypeInfo}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


@JsonSerialize(using = classOf[DataOutputFormatSerializer])
@JsonDeserialize(using = classOf[DataOutputFormatDeserializer])
trait DataOutputFormat[T] extends GenericTypeInfoProvider with SetGenericTypeInfo with Serializable {
  def writeValue(value: T, outputStream: OutputStream): Unit

  def writeValues(values: TraversableOnce[T], outputStream: OutputStream): Unit

  def writeValueAsBytes(value: T): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    this.writeValue(value, stream)
    stream.toByteArray
  }

  def writeValuesAsBytes(values: TraversableOnce[T]): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    this.writeValues(values, stream)
    stream.toByteArray
  }
}


class DataOutputFormatDeserializer extends GenericTypedJsonDeserializer[DataOutputFormat[_]]("com.amazon.milan.dataformats")


class DataOutputFormatSerializer extends GenericTypedJsonSerializer[DataOutputFormat[_]]
