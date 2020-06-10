package com.amazon.milan.dataformats

import java.io.{ByteArrayOutputStream, OutputStream}

import com.amazon.milan.serialization.{GenericTypeInfoProvider, GenericTypedJsonDeserializer, GenericTypedJsonSerializer, SetGenericTypeInfo}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * Trait for output formatters that control how objects are written to a stream.
 *
 * @tparam T The type of objects being written.
 */
@JsonSerialize(using = classOf[DataOutputFormatSerializer])
@JsonDeserialize(using = classOf[DataOutputFormatDeserializer])
trait DataOutputFormat[T] extends GenericTypeInfoProvider with SetGenericTypeInfo with Serializable {
  /**
   * Writes a single value to a stream.
   *
   * @param value        The value to write.
   * @param outputStream The destination stream.
   */
  def writeValue(value: T, outputStream: OutputStream): Unit

  /**
   * Writes multiple values to a stream.
   *
   * @param values       The values to write.
   * @param outputStream The destination stream.
   */
  def writeValues(values: TraversableOnce[T], outputStream: OutputStream): Unit

  /**
   * Gets a byte array containing the serialized representation of an object.
   *
   * @param value The object to serialize.
   * @return A byte array containing the serialized object.
   */
  def writeValueAsBytes(value: T): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    this.writeValue(value, stream)
    stream.toByteArray
  }

  /**
   * Gets a byte array containing the serialized representations of multiple objects.
   *
   * @param values The objects to serialize.
   * @return A byte array containing the serialized objects.
   */
  def writeValuesAsBytes(values: TraversableOnce[T]): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    this.writeValues(values, stream)
    stream.toByteArray
  }
}


class DataOutputFormatDeserializer extends GenericTypedJsonDeserializer[DataOutputFormat[_]]("com.amazon.milan.dataformats")


class DataOutputFormatSerializer extends GenericTypedJsonSerializer[DataOutputFormat[_]]
