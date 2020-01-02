package com.amazon.milan.flink.application.sources

import java.time.Duration

import com.amazon.milan.application
import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.application.sinks.SingletonMemorySink
import com.amazon.milan.application.sources.ListDataSource
import com.amazon.milan.flink.application.FlinkDataSource
import com.amazon.milan.flink.testing._
import com.amazon.milan.flink.testutil.{IntRecord, KeyValueRecord}
import com.amazon.milan.lang.{Stream, StreamGraph}
import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.testing.Concurrent
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
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

  @Test
  def test_FlinkListDataSource_CreateEmptyDataSource_OutputsNoRecord(): Unit = {
    val inputStream = Stream.of[KeyValueRecord]

    val graph = new StreamGraph(inputStream)

    val config = new ApplicationConfiguration()
    config.setSource(inputStream, new ListDataSource[KeyValueRecord](List()))

    val outputSink = new SingletonMemorySink[KeyValueRecord]()
    config.addSink(inputStream, outputSink)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    compileFromSerialized(graph, config, env)

    assertTrue(
      Concurrent.executeAndWait(
        () => env.execute(),
        () => !outputSink.hasValues,
        Duration.ofSeconds(10)))

    assertEquals(0, outputSink.getRecordCount)
  }

  @Test
  def test_FlinkListDataSource_CreateDataSource_WithOneRecord_OutputsThatRecord(): Unit = {
    val inputStream = Stream.of[KeyValueRecord]

    val graph = new StreamGraph(inputStream)

    val config = new ApplicationConfiguration()
    val record = KeyValueRecord("key", "value")
    config.setSource(inputStream, new ListDataSource(List(record)))

    val outputSink = new SingletonMemorySink[KeyValueRecord]()
    config.addSink(inputStream, outputSink)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    compileFromSerialized(graph, config, env)

    assertTrue(
      Concurrent.executeAndWait(
        () => env.execute(),
        () => outputSink.hasValues,
        Duration.ofSeconds(10)))

    assertEquals(1, outputSink.getValues.length)

    val output = outputSink.getValues.head
    assertEquals(record, output)
  }
}
