package com.amazon.milan.aws.serverless.runtime

import com.amazon.milan.serialization.MilanObjectMapper
import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

object MilanAttributeValue {
  def S(s: String): MilanDynamoDbAttributeValue =
    MilanDynamoDbAttributeValue(s, null, None, null, null, null, null, s == null)

  def N(n: String): MilanDynamoDbAttributeValue =
    MilanDynamoDbAttributeValue(null, n, None, null, null, null, null, n == null)

  def BOOL(b: Boolean): MilanDynamoDbAttributeValue =
    MilanDynamoDbAttributeValue(null, null, Some(b), null, null, null, null, isNull = false)

  def SS(ss: List[String]): MilanDynamoDbAttributeValue =
    MilanDynamoDbAttributeValue(null, null, None, ss, null, null, null, ss == null)

  def SS(ss: String*): MilanDynamoDbAttributeValue =
    MilanDynamoDbAttributeValue(null, null, None, ss.toList, null, null, null, ss == null)

  def NS(ns: List[String]): MilanDynamoDbAttributeValue =
    MilanDynamoDbAttributeValue(null, null, None, null, ns, null, null, ns == null)

  def NS(ns: String*): MilanDynamoDbAttributeValue =
    MilanDynamoDbAttributeValue(null, null, None, null, ns.toList, null, null, ns == null)

  def L(l: List[MilanDynamoDbAttributeValue]): MilanDynamoDbAttributeValue =
    MilanDynamoDbAttributeValue(null, null, None, null, null, l, null, l == null)

  def L(l: MilanDynamoDbAttributeValue*): MilanDynamoDbAttributeValue =
    MilanDynamoDbAttributeValue(null, null, None, null, null, l.toList, null, l == null)

  def M(m: Map[String, MilanDynamoDbAttributeValue]): MilanDynamoDbAttributeValue =
    MilanDynamoDbAttributeValue(null, null, None, null, null, null, m, m == null)
}

@JsonDeserialize(using = classOf[AttributeValueJsonDeserializer])
case class MilanDynamoDbAttributeValue(S: String,
                                       N: String,
                                       BOOL: Option[Boolean],
                                       SS: List[String],
                                       NS: List[String],
                                       L: List[MilanDynamoDbAttributeValue],
                                       M: Map[String, MilanDynamoDbAttributeValue],
                                       isNull: Boolean) {
  def isString: Boolean =
    this.S != null

  def isNumber: Boolean =
    this.N != null

  def isStringSet: Boolean =
    this.SS != null

  def isNumberSet: Boolean =
    this.NS != null

  def isList: Boolean =
    this.L != null

  def isMap: Boolean =
    this.M != null

  def isBoolean: Boolean =
    this.BOOL.isDefined
}


class AttributeValueJsonDeserializer extends JsonDeserializer[MilanDynamoDbAttributeValue] {
  override def deserialize(parser: JsonParser, context: DeserializationContext): MilanDynamoDbAttributeValue = {
    val token = parser.nextToken()
    if (token == JsonToken.FIELD_NAME) {
      val fieldName = parser.getText()
      val attributeValue = this.readAttributeValue(fieldName, parser, context)

      // The next token should be the END_OBJECT that closes the AttributeValue.
      val lastToken = parser.nextToken()
      assert(lastToken == JsonToken.END_OBJECT)

      attributeValue
    }
    else {
      throw new AssertionError(s"Expected FIELD_NAME token, but found '$token'.'")
    }
  }

  private def readAttributeValue(fieldName: String,
                                 parser: JsonParser,
                                 context: DeserializationContext): MilanDynamoDbAttributeValue = {
    val valueToken = parser.nextToken()
    if (valueToken == JsonToken.VALUE_NULL) {
      MilanDynamoDbAttributeValue(null, null, None, null, null, null, null, isNull = true)
    }
    else {
      fieldName match {
        case "S" =>
          this.readStringValue(parser)

        case "N" =>
          this.readNumberValue(parser)

        case "BOOL" =>
          this.readBoolValue(parser)

        case "SS" =>
          this.readStringSet(parser, context)

        case "NS" =>
          this.readNumberSet(parser, context)

        case "L" =>
          this.readList(parser, context)

        case "M" =>
          this.readMap(parser, context)
      }
    }
  }

  private def readStringValue(parser: JsonParser): MilanDynamoDbAttributeValue = {
    val s = parser.getText()
    MilanDynamoDbAttributeValue(s, null, None, null, null, null, null, s == null)
  }

  private def readNumberValue(parser: JsonParser): MilanDynamoDbAttributeValue = {
    val n = parser.getText()
    MilanDynamoDbAttributeValue(null, n, None, null, null, null, null, n == null)
  }

  private def readBoolValue(parser: JsonParser): MilanDynamoDbAttributeValue = {
    val b = parser.currentToken() == JsonToken.VALUE_TRUE
    MilanDynamoDbAttributeValue(null, null, Some(b), null, null, null, null, isNull = false)
  }

  private def readStringSet(parser: JsonParser, context: DeserializationContext): MilanDynamoDbAttributeValue = {
    val ss = context.readValue[List[String]](parser, classOf[List[String]])
    MilanDynamoDbAttributeValue(null, null, None, ss, null, null, null, ss == null)
  }

  private def readNumberSet(parser: JsonParser, context: DeserializationContext): MilanDynamoDbAttributeValue = {
    val ns = context.readValue[List[String]](parser, classOf[List[String]])
    MilanDynamoDbAttributeValue(null, null, None, null, ns, null, null, ns == null)
  }

  private def readList(parser: JsonParser, context: DeserializationContext): MilanDynamoDbAttributeValue = {
    val l = context.readValue[List[MilanDynamoDbAttributeValue]](parser, classOf[List[MilanDynamoDbAttributeValue]])
    MilanDynamoDbAttributeValue(null, null, None, null, null, l, null, l == null)
  }

  private def readMap(parser: JsonParser, context: DeserializationContext): MilanDynamoDbAttributeValue = {
    val javaType =
      MilanObjectMapper.getTypeFactory.constructMapLikeType(
        classOf[Map[String, MilanDynamoDbAttributeValue]],
        classOf[String],
        classOf[MilanDynamoDbAttributeValue])

    val m = context.readValue[Map[String, MilanDynamoDbAttributeValue]](parser, javaType)
    MilanDynamoDbAttributeValue(null, null, None, null, null, null, m, m == null)
  }
}
