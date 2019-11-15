package com.amazon.milan.serialization

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


class GenericTypedJsonSerializer[T <: GenericTypeInfoProvider] extends JsonSerializer[T] {
  private val logger = Logger(LoggerFactory.getLogger(getClass))

  override def serialize(value: T,
                         jsonGenerator: JsonGenerator,
                         serializerProvider: SerializerProvider): Unit = {
    val typeName = value.getTypeName
    val genericArgs = value.getGenericArguments
    logger.info(s"Serializing type '$typeName[${genericArgs.map(_.fullName).mkString(", ")}]'.")

    jsonGenerator.writeStartObject()
    jsonGenerator.writeStringField("_type", value.getTypeName)
    jsonGenerator.writeObjectField("_genericArgs", value.getGenericArguments)
    jsonGenerator.writeObject(value)
    jsonGenerator.writeEndObject()
  }
}
