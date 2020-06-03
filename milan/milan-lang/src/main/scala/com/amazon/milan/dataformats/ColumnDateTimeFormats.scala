package com.amazon.milan.dataformats

import java.time.Instant
import java.time.format.DateTimeFormatter

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}


class ColumnDateTimeFormatsModule(dateTimeFormats: Map[String, DateTimeFormatter]) extends SimpleModule {
  this.addSerializer(classOf[Instant], new ColumnDateTimeFormatsInstantSerializer(this.dateTimeFormats))
}


class ColumnDateTimeFormatsInstantSerializer(dateTimeFormats: Map[String, DateTimeFormatter]) extends JsonSerializer[Instant] {
  override def serialize(value: Instant,
                         jsonGenerator: JsonGenerator,
                         serializerProvider: SerializerProvider): Unit = {
    val contextName = jsonGenerator.getOutputContext.getCurrentName

    val dateTimeFormatter =
      this.dateTimeFormats.get(contextName) match {
        case Some(formatter) => formatter
        case None => DateTimeFormatter.ISO_INSTANT
      }

    jsonGenerator.writeString(dateTimeFormatter.format(value))
  }
}