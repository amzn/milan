package com.amazon.milan.aws.serverless.runtime

import org.junit.Assert._
import org.junit.Test


@Test
class TestDynamoDbImage {
  @Test
  def test_DynamoDbImage_ToJson_FlatObject_ReturnsCorrectlyFormattedOutput(): Unit = {
    val image =
      Map(
        "n" -> MilanAttributeValue.N("5"),
        "s" -> MilanAttributeValue.S("text"),
        "b" -> MilanAttributeValue.BOOL(true)
      )

    val json = DynamoDbImage.toJson(image)
    val expected =
      """{
        |  "n": 5,
        |  "s": "text",
        |  "b": true
        |}""".stripMargin
    assertEquals(expected, json)
  }

  @Test
  def test_DynamoDbImage_ToJson_NumberArrayField_ReturnsCorrectlyFormattedOutput(): Unit = {
    val image =
      Map(
        "ns" -> MilanAttributeValue.NS("1", "2", "3", "4")
      )

    val json = DynamoDbImage.toJson(image)
    val expected =
      """{
        |  "ns": [
        |    1,
        |    2,
        |    3,
        |    4
        |  ]
        |}""".stripMargin
    assertEquals(expected, json)
  }

  @Test
  def test_DynamoDbImage_ToJson_StringArrayField_ReturnsCorrectlyFormattedOutput(): Unit = {
    val image =
      Map(
        "ss" -> MilanAttributeValue.SS("1", "2", "3", "4")
      )

    val json = DynamoDbImage.toJson(image)
    val expected =
      """{
        |  "ss": [
        |    "1",
        |    "2",
        |    "3",
        |    "4"
        |  ]
        |}""".stripMargin
    assertEquals(expected, json)
  }

  @Test
  def test_DynamoDbImage_ToJson_NestedObject_ReturnsCorrectlyFormattedOutput(): Unit = {
    val image =
      Map(
        "n" -> MilanAttributeValue.N("5"),
        "s" -> MilanAttributeValue.S("text"),
        "b" -> MilanAttributeValue.BOOL(true),
        "m" -> MilanAttributeValue.M(Map(
          "n" -> MilanAttributeValue.N("5"),
          "s" -> MilanAttributeValue.S("text"),
          "b" -> MilanAttributeValue.BOOL(true)
        ))
      )

    val json = DynamoDbImage.toJson(image)
    val expected =
      """{
        |  "n": 5,
        |  "s": "text",
        |  "b": true,
        |  "m": {
        |    "n": 5,
        |    "s": "text",
        |    "b": true
        |  }
        |}""".stripMargin
    assertEquals(expected, json)
  }
}
