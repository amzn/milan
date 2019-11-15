package com.amazon.milan.serialization

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}


class TypedJsonSerializer[T <: TypeInfoProvider] extends JsonSerializer[T] {
  override def serialize(value: T,
                         jsonGenerator: JsonGenerator,
                         serializerProvider: SerializerProvider): Unit = {
    jsonGenerator.writeStartObject()
    jsonGenerator.writeStringField("_type", value.getJsonTypeName)
    jsonGenerator.writeObject(value)
    jsonGenerator.writeEndObject()
  }
}
