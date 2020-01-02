package com.amazon.milan.dataformats

import java.io.InputStream

import com.amazon.milan.serialization.{GenericTypeInfoProvider, GenericTypedJsonDeserializer, GenericTypedJsonSerializer, SetGenericTypeInfo}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


@JsonSerialize(using = classOf[DataFormatSerializer])
@JsonDeserialize(using = classOf[DataFormatDeserializer])
trait DataFormat[T] extends GenericTypeInfoProvider with SetGenericTypeInfo with Serializable {
  /**
   * Reads a value in a byte array.
   *
   * @param bytes  A byte array.
   * @param offset The start of the value in the array.
   * @param length The length of the value in the array.
   * @return The value read from the array.
   */
  def readValue(bytes: Array[Byte], offset: Int, length: Int): T

  /**
   * Reads a sequence of encoded values from a stream.
   *
   * @param stream The stream containing the sequence of encoded values.
   * @return A [[TraversableOnce]] that yields the values read from the stream.
   */
  def readValues(stream: InputStream): TraversableOnce[T]
}


class DataFormatDeserializer extends GenericTypedJsonDeserializer[DataFormat[_]]("com.amazon.milan.dataformats")


class DataFormatSerializer extends GenericTypedJsonSerializer[DataFormat[_]]
