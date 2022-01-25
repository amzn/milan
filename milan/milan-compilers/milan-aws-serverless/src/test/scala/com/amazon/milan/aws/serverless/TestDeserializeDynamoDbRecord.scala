package com.amazon.milan.aws.serverless

import com.amazon.milan.aws.serverless.runtime.{MilanAttributeValue, DynamoDbImage}
import com.amazon.milan.serialization._
import org.junit.Assert._
import org.junit.Test


object TestDeserializeDynamoDbRecord {
  case class FlatRecordOnlyScalars(id: String, intValue: Int, stringValue: String, boolValue: Boolean)

  case class FlatRecordWithCollections(intValue: Int,
                                       stringValue: String,
                                       numberSet: List[Int],
                                       stringSet: List[String],
                                       intMap: Map[String, Int])

  case class NestedRecord(id: String,
                          boolValue: Boolean,
                          recordValue: FlatRecordWithCollections,
                          recordList: List[FlatRecordWithCollections])
}

import com.amazon.milan.aws.serverless.TestDeserializeDynamoDbRecord._


@Test
class TestDeserializeDynamoDbRecord {
  @Test
  def test_DeserializeDynamoDbRecord_WithFlatRecordOnlyScalars(): Unit = {
    val newImage = Map(
      "id" -> MilanAttributeValue.S("id"),
      "intValue" -> MilanAttributeValue.N("5"),
      "stringValue" -> MilanAttributeValue.S("text"),
      "boolValue" -> MilanAttributeValue.BOOL(true),
    )

    val valueJson = DynamoDbImage.toJson(newImage)
    val value = MilanObjectMapper.readValue[FlatRecordOnlyScalars](valueJson, classOf[FlatRecordOnlyScalars])

    assertEquals(FlatRecordOnlyScalars("id", 5, "text", boolValue = true), value)
  }

  @Test
  def test_DeserializeDynamoDbRecord_WithFlatRecordWithCollections(): Unit = {
    val newImage = Map(
      "intValue" -> MilanAttributeValue.N("2"),
      "stringValue" -> MilanAttributeValue.S("text"),
      "numberSet" -> MilanAttributeValue.NS("1", "2", "3"),
      "stringSet" -> MilanAttributeValue.SS("a", "b", "c"),
      "intMap" -> MilanAttributeValue.M(
        Map(
          "x" -> MilanAttributeValue.N("1"),
          "y" -> MilanAttributeValue.N("2"),
        )
      )
    )

    val valueJson = DynamoDbImage.toJson(newImage)
    val value = MilanObjectMapper.readValue[FlatRecordWithCollections](valueJson, classOf[FlatRecordWithCollections])

    val expected = FlatRecordWithCollections(
      2,
      "text",
      List(1, 2, 3),
      List("a", "b", "c"),
      Map("x" -> 1, "y" -> 2)
    )
    assertEquals(expected, value)
  }

  @Test
  def test_DeserializeDynamoDbRecord_NestedRecord(): Unit = {
    val newImage = Map(
      "id" -> MilanAttributeValue.S("recordId"),
      "boolValue" -> MilanAttributeValue.BOOL(true),
      "recordValue" -> MilanAttributeValue.M(
        Map(
          "intValue" -> MilanAttributeValue.N("2"),
          "stringValue" -> MilanAttributeValue.S("text"),
          "numberSet" -> MilanAttributeValue.NS("1", "2", "3"),
          "stringSet" -> MilanAttributeValue.SS("a", "b", "c"),
          "intMap" -> MilanAttributeValue.M(
            Map(
              "x" -> MilanAttributeValue.N("1"),
              "y" -> MilanAttributeValue.N("2"),
            )
          )
        )
      ),
      "recordList" -> MilanAttributeValue.L(
        MilanAttributeValue.M(
          Map(
            "intValue" -> MilanAttributeValue.N("5"),
            "stringValue" -> MilanAttributeValue.S("text1"),
            "numberSet" -> MilanAttributeValue.NS("1", "2", "3"),
            "stringSet" -> MilanAttributeValue.SS("a", "b", "c"),
            "intMap" -> MilanAttributeValue.M(
              Map(
                "x" -> MilanAttributeValue.N("1"),
                "y" -> MilanAttributeValue.N("2"),
              )
            )
          )
        ),
        MilanAttributeValue.M(
          Map(
            "intValue" -> MilanAttributeValue.N("6"),
            "stringValue" -> MilanAttributeValue.S("text2"),
            "numberSet" -> MilanAttributeValue.NS("5", "6", "7"),
            "stringSet" -> MilanAttributeValue.SS("d", "e", "f"),
            "intMap" -> MilanAttributeValue.M(
              Map(
                "a" -> MilanAttributeValue.N("1"),
                "b" -> MilanAttributeValue.N("2"),
              )
            )
          )
        )
      )
    )

    val valueJson = DynamoDbImage.toJson(newImage)
    val value = MilanObjectMapper.readValue[NestedRecord](valueJson, classOf[NestedRecord])

    val expected =
      NestedRecord(
        "recordId",
        boolValue = true,
        recordValue =
          FlatRecordWithCollections(
            2,
            "text",
            List(1, 2, 3),
            List("a", "b", "c"),
            Map("x" -> 1, "y" -> 2)
          ),
        recordList =
          List(
            FlatRecordWithCollections(
              5,
              "text1",
              List(1, 2, 3),
              List("a", "b", "c"),
              Map("x" -> 1, "y" -> 2)
            ),
            FlatRecordWithCollections(
              6,
              "text2",
              List(5, 6, 7),
              List("d", "e", "f"),
              Map("a" -> 1, "b" -> 2)
            ),
          )
      )
    assertEquals(expected, value)
  }
}
