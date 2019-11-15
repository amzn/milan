package com.amazon.milan.serialization

import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


class GenericTypedJsonDeserializer[T <: SetGenericTypeInfo](typeNameTransformer: String => String)
  extends JsonDeserializer[T] {

  private val logger = Logger(LoggerFactory.getLogger(getClass))

  def this(packageName: String) {
    this(typeName => s"$packageName.$typeName")
  }

  override def deserialize(parser: JsonParser, context: DeserializationContext): T = {
    assert(parser.nextFieldName() == "_type")

    val typeName = this.typeNameTransformer(parser.nextTextValue())

    assert(parser.nextFieldName() == "_genericArgs")
    parser.nextToken()

    val genericArgs = context.readValue[Array[TypeDescriptor[_]]](parser, classOf[Array[TypeDescriptor[_]]]).toList
    parser.nextToken()

    logger.info(s"Deserializing type '$typeName[${genericArgs.map(_.fullName).mkString(", ")}]'.")

    val cls = getClass.getClassLoader.loadClass(typeName).asInstanceOf[Class[T]]
    val javaType = new JavaTypeFactory(context.getTypeFactory).makeJavaType(cls, genericArgs)

    val value = context.readValue[T](parser, javaType)
    value.setGenericArguments(genericArgs)

    value
  }
}
