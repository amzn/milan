package com.amazon.milan.serialization

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}


class TypedJsonSerializer[T <: TypeInfoProvider] extends JsonSerializer[T] {
  override def serialize(value: T,
                         jsonGenerator: JsonGenerator,
                         serializerProvider: SerializerProvider): Unit = {
    val typeName = value.getJsonTypeName

    try {
      jsonGenerator.writeStartObject()
      jsonGenerator.writeStringField("_type", typeName)
      jsonGenerator.writeObject(value)
      jsonGenerator.writeEndObject()
    }
    catch {
      case ex: Throwable => throw new JsonSerializationException(s"Error serializing object of type $typeName.", ex)
    }
  }
}


class JsonSerializationException(message: String, cause: Throwable) extends Exception(message, cause) {

}
