package com.amazon.milan.serialization

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


class TypedJsonDeserializer[T](packageName: String) extends JsonDeserializer[T] {
  private val logger = Logger(LoggerFactory.getLogger(getClass))

  override def deserialize(parser: JsonParser, context: DeserializationContext): T = {
    assert(parser.nextFieldName() == "_type")

    val typeName = packageName + "." + parser.nextTextValue()
    logger.debug(s"Deserializing type '$typeName'.")

    parser.nextToken()

    val cls = getClass.getClassLoader.loadClass(typeName).asInstanceOf[Class[T]]
    context.readValue[T](parser, cls)
  }
}
