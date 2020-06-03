package com.amazon.milan.dataformats

import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.temporal.{TemporalAccessor, TemporalQuery}
import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}


class InstantModule extends SimpleModule {
  this.addDeserializer[Instant](classOf[Instant], new MilanInstantDeserializer)
}


class MilanInstantDeserializer extends JsonDeserializer[Instant] {
  private val formatsToTry = List(
    DateTimeFormatter.ISO_INSTANT,
    DateTimeFormatter.ISO_DATE_TIME,
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
    DateTimeFormatter.ISO_DATE)

  override def deserialize(parser: JsonParser, context: DeserializationContext): Instant = {
    val textValue = parser.getText
    this.parseInstant(textValue)
  }

  private val createInstant = new TemporalQuery[Instant] {
    override def queryFrom(temporal: TemporalAccessor): Instant = LocalDateTime.from(temporal).toInstant(ZoneOffset.UTC)
  }


  private def parseInstant(dateTimeString: String): Instant = {
    // Try a bunch of formats.
    // TODO: This is awful but will do for now.
    formatsToTry.map(formatter => this.tryParseFormat(dateTimeString, formatter))
      .filter(_.isDefined)
      .map(_.get)
      .headOption match {
      case Some(instant) =>
        instant

      case None =>
        throw new DateTimeParseException(s"Unable to parse datetime string '$dateTimeString'.", dateTimeString, 0)
    }
  }

  private def tryParseFormat(dateTimeString: String,
                             formatter: DateTimeFormatter): Option[Instant] = {
    try {
      Some(formatter.parse(dateTimeString, this.createInstant))
    }
    catch {
      case _: DateTimeParseException =>
        None
    }
  }
}
