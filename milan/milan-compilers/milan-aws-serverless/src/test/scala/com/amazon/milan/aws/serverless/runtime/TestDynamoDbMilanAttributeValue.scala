package com.amazon.milan.aws.serverless.runtime

import com.amazon.milan.serialization.MilanObjectMapper
import org.junit.Assert._
import org.junit.Test


@Test
class TestDynamoDbMilanAttributeValue {
  @Test
  def test_DynamoDbAttributeValue_Deserialize_StringAttribute(): Unit = {
    val json = """{"S": "text"}"""
    val value = MilanObjectMapper.readValue[MilanDynamoDbAttributeValue](json, classOf[MilanDynamoDbAttributeValue])
    assertEquals(MilanAttributeValue.S("text"), value)
  }

  @Test
  def test_DynamoDbAttributeValue_Deserialize_NumberAttribute(): Unit = {
    val json = """{"N": "5"}"""
    val value = MilanObjectMapper.readValue[MilanDynamoDbAttributeValue](json, classOf[MilanDynamoDbAttributeValue])
    assertEquals(MilanAttributeValue.N("5"), value)
  }

  @Test
  def test_DynamoDbAttributeValue_Deserialize_BoolAttribute(): Unit = {
    val json = """{"BOOL": true}"""
    val value = MilanObjectMapper.readValue[MilanDynamoDbAttributeValue](json, classOf[MilanDynamoDbAttributeValue])
    assertEquals(MilanAttributeValue.BOOL(true), value)
  }

  @Test
  def test_DynamoDbAttributeValue_Deserialize_StringSetAttribute(): Unit = {
    val json = """{"SS": ["a", "b", "c"]}"""
    val value = MilanObjectMapper.readValue[MilanDynamoDbAttributeValue](json, classOf[MilanDynamoDbAttributeValue])
    assertEquals(MilanAttributeValue.SS("a", "b", "c"), value)
  }

  @Test
  def test_DynamoDbAttributeValue_Deserialize_NumberSetAttribute(): Unit = {
    val json = """{"NS": ["1", "2", "3"]}"""
    val value = MilanObjectMapper.readValue[MilanDynamoDbAttributeValue](json, classOf[MilanDynamoDbAttributeValue])
    assertEquals(MilanAttributeValue.NS("1", "2", "3"), value)
  }

  @Test
  def test_DynamoDbAttributeValue_Deserialize_MapAttribute(): Unit = {
    val json = """{"M": {"a": {"S": "text"}, "b": {"N": "5"}}}"""
    val value = MilanObjectMapper.readValue[MilanDynamoDbAttributeValue](json, classOf[MilanDynamoDbAttributeValue])
    val expected = MilanAttributeValue.M(Map(
      "a" -> MilanAttributeValue.S("text"),
      "b" -> MilanAttributeValue.N("5")
    ))
    assertEquals(expected, value)
  }

  @Test
  def test_DynamoDbAttributeValue_Deserialize_MapOfAttributeValues(): Unit = {
    val json = """{"a": {"S": "text"}, "b": {"N": "5"}}"""
    val javaType =
      MilanObjectMapper.getTypeFactory.constructMapLikeType(
        classOf[Map[String, MilanDynamoDbAttributeValue]],
        classOf[String],
        classOf[MilanDynamoDbAttributeValue])
    val value = MilanObjectMapper.readValue[Map[String, MilanDynamoDbAttributeValue]](json, javaType)
    val expected = Map("a" -> MilanAttributeValue.S("text"), "b" -> MilanAttributeValue.N("5"))
    assertEquals(expected, value)
  }

  @Test
  def test_DynamoDbAttributeValue_Deserialize_AllFieldTypes(): Unit = {
    val json =
      """
        |{
        |  "ss": {
        |      "SS": [
        |          "a",
        |          "b"
        |      ]
        |  },
        |  "b": {
        |      "BOOL": true
        |  },
        |  "s": {
        |      "S": "text2"
        |  },
        |  "ns": {
        |      "NS": [
        |          "3",
        |          "2",
        |          "1"
        |      ]
        |  },
        |  "i": {
        |      "N": "51"
        |  },
        |  "m": {
        |      "M": {
        |          "a": {
        |              "N": "1"
        |              },
        |              "b": {
        |                  "S": "2"
        |              }
        |          }
        |    },
        |    "o": {
        |        "M": {
        |            "ss": {
        |                "SS": [
        |                    "x",
        |                    "y"
        |                ]
        |            },
        |            "i": {
        |                "N": "3"
        |            }
        |        }
        |    }
        |  }
        |}
        |""".stripMargin

    val javaType =
      MilanObjectMapper.getTypeFactory.constructMapLikeType(
        classOf[Map[String, MilanDynamoDbAttributeValue]],
        classOf[String],
        classOf[MilanDynamoDbAttributeValue])

    val value = MilanObjectMapper.readValue[Map[String, MilanDynamoDbAttributeValue]](json, javaType)

    assertEquals(MilanAttributeValue.S("text2"), value("s"))
  }
}
