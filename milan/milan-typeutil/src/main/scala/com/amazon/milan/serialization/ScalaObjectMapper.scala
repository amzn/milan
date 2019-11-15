package com.amazon.milan.serialization

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.reflect.{ClassTag, classTag}


class ScalaObjectMapper(config: DataFormatConfiguration) extends ObjectMapper {
  this.registerModule(DefaultScalaModule)
  this.registerModule(new JavaTimeModule())

  this.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, config.isEnabled(DataFormatFlags.FailOnUnknownProperties))
  this.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true)
  this.configure(DeserializationFeature.READ_ENUMS_USING_TO_STRING, true)

  def this() {
    this(DataFormatConfiguration.default)
  }

  def copy[T: ClassTag](value: T): T = {
    val json = this.writeValueAsString(value)
    this.readValue[T](json, classTag[T].runtimeClass.asInstanceOf[Class[T]])
  }
}


object ScalaObjectMapper extends ScalaObjectMapper()
