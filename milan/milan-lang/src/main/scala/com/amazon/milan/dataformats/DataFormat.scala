package com.amazon.milan.dataformats

import com.amazon.milan.serialization.{GenericTypeInfoProvider, GenericTypedJsonDeserializer, GenericTypedJsonSerializer, SetGenericTypeInfo}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


@JsonSerialize(using = classOf[DataFormatSerializer])
@JsonDeserialize(using = classOf[DataFormatDeserializer])
trait DataFormat[T] extends GenericTypeInfoProvider with SetGenericTypeInfo with Serializable {
  def readValue(bytes: Array[Byte], offset: Int, length: Int): T
}


class DataFormatDeserializer extends GenericTypedJsonDeserializer[DataFormat[_]]("com.amazon.milan.dataformats")


class DataFormatSerializer extends GenericTypedJsonSerializer[DataFormat[_]]
