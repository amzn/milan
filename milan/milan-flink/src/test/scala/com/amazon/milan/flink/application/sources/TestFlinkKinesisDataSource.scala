package com.amazon.milan.flink.application.sources

import com.amazon.milan.dataformats.{CsvDataFormat, JsonDataFormat}
import com.amazon.milan.serialization.ObjectSerialization
import org.apache.flink.api.scala.createTypeInformation
import org.junit.Assert._
import org.junit.Test


object TestFlinkKinesisDataSource {

  case class StringRecord(value: String)

}

import com.amazon.milan.flink.application.sources.TestFlinkKinesisDataSource._


@Test
class TestFlinkKinesisDataSource {
  @Test
  def test_KinesisDataSource_WithJsonDataFormat_DeserializesToEquivalentObject(): Unit = {
    val original = new FlinkKinesisDataSource[StringRecord](
      "streamName",
      "region",
      new JsonDataFormat[StringRecord](),
      createTypeInformation[StringRecord])

    val copy = ObjectSerialization.deserialize[FlinkKinesisDataSource[StringRecord]](ObjectSerialization.serialize(original))
    assertEquals(original, copy)
  }

  @Test
  def test_KinesisDataSource_WithCsvDataFormat_DeserializesToEquivalentObject(): Unit = {
    val format = new CsvDataFormat[StringRecord](Array("value"))
    val original = new FlinkKinesisDataSource[StringRecord](
      "streamName",
      "region",
      format,
      createTypeInformation[StringRecord])

    val copy = ObjectSerialization.deserialize[FlinkKinesisDataSource[StringRecord]](ObjectSerialization.serialize(original))
    assertEquals(original, copy)
  }
}
