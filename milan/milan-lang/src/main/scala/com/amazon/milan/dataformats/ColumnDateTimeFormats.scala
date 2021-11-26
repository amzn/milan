package com.amazon.milan.dataformats

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}

import java.time.Instant
import java.time.format.DateTimeFormatter


/**
 * A Jackson databind module that applies [[DateTimeFormatter]] objects to specific fields.
 *
 * @param dateTimeFormats A map of field names to [[DateTimeFormatter]] objects to be used for those fields.
 */
class ColumnDateTimeFormatsModule(dateTimeFormats: Map[String, DateTimeFormatter]) extends SimpleModule {
  this.addSerializer(classOf[Instant], new ColumnDateTimeFormatsInstantSerializer(this.dateTimeFormats))
}


/**
 * A [[JsonSerializer]] that applies specific [[DateTimeFormatter]]s to specific output fields.
 *
 * @param dateTimeFormats A map of field names to [[DateTimeFormatter]] objects to be used for those fields.
 */
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
