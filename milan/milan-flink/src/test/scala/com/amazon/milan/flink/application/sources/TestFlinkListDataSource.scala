package com.amazon.milan.flink.application.sources

import com.amazon.milan.application
import com.amazon.milan.flink.application.FlinkDataSource
import com.amazon.milan.flink.testing._
import com.amazon.milan.serialization.ScalaObjectMapper
import org.junit.Assert._
import org.junit.Test


object TestFlinkListDataSource {

  case class CaseClassRecord(i: Int)

}

import com.amazon.milan.flink.application.sources.TestFlinkListDataSource._


@Test
class TestFlinkListDataSource {
  @Test
  def test_FlinkListDataSource_DeserializeViaObjectMapper_WithCaseClass_ReturnsFlinkListDataSourceWithTheSameValuesAsOriginal(): Unit = {
    val original = new application.sources.ListDataSource(List(CaseClassRecord(0), CaseClassRecord(1)))
    val mapper = new ScalaObjectMapper()
    val json = mapper.writeValueAsString(original)
    val copy = mapper.readValue[FlinkDataSource[_]](json, classOf[FlinkDataSource[_]]).asInstanceOf[FlinkListDataSource[_]]
    assertEquals(original.values, copy.values)
  }

  @Test
  def test_FlinkListDataSource_DeserializeViaObjectMapper_WithStandardClass_ReturnsFlinkListDataSourceWithTheSameValuesAsOriginal(): Unit = {
    val original = new application.sources.ListDataSource(List(new IntRecord(0), new IntRecord(1)))
    val mapper = new ScalaObjectMapper()
    val json = mapper.writeValueAsString(original)
    val copy = mapper.readValue[FlinkDataSource[_]](json, classOf[FlinkDataSource[_]]).asInstanceOf[FlinkListDataSource[_]]
    assertEquals(original.values, copy.values)
  }
}
