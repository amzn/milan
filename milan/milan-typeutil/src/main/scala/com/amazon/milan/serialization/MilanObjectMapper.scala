package com.amazon.milan.serialization

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

import scala.reflect.{ClassTag, classTag}


/**
 * A Jackson [[ObjectMapper]] extended with Scala type support and Java8 time support.
 *
 * @param config A [[DataFormatConfiguration]] that controls some of the mapper behavior.
 */
class MilanObjectMapper(config: DataFormatConfiguration) extends ObjectMapper with ScalaObjectMapper {
  this.registerModule(DefaultScalaModule)
  this.registerModule(new JavaTimeModule())

  this.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, config.isEnabled(DataFormatFlags.FailOnUnknownProperties))
  this.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true)
  this.configure(DeserializationFeature.READ_ENUMS_USING_TO_STRING, true)
  this.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)

  def this() {
    this(DataFormatConfiguration.default)
  }

  /**
   * Copies an object using this object mapper.
   *
   * @param value An object to copy.
   * @tparam T The type of the object being copied.
   * @return A copy of the object, created by serializing and deserializing the input value.
   */
  def copy[T: ClassTag](value: T): T = {
    val json = this.writeValueAsString(value)
    this.readValue[T](json, classTag[T].runtimeClass.asInstanceOf[Class[T]])
  }
}


object MilanObjectMapper extends MilanObjectMapper()
