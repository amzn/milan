package com.amazon.milan.flink.compiler

import java.time.Duration

import com.amazon.milan.application
import com.amazon.milan.flink.application.FlinkApplicationConfiguration
import com.amazon.milan.flink.application.sinks.FlinkSingletonMemorySink
import com.amazon.milan.flink.application.sources.FlinkListDataSource
import com.amazon.milan.flink.testutil._
import com.amazon.milan.lang.{Stream, StreamGraph}
import com.amazon.milan.test.IntRecord
import com.amazon.milan.testing.Concurrent
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._


@Test
class TestFlinkCompiler {
  @Test
  def test_FlinkCompiler_Compile_WithSingleExternalStreamWithListSourceAndMemorySink_ThenExecuteEnvironment_AddsSourceRecordsToSink(): Unit = {
    val stream = Stream.of[IntRecord]

    val graph = new StreamGraph(stream)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val config = new FlinkApplicationConfiguration()
    val records = List(IntRecord(1), IntRecord(2))
    config.setSource(stream, FlinkListDataSource.create(records))

    val sink = FlinkSingletonMemorySink.create[IntRecord]
    config.addSink(stream, sink)

    FlinkCompiler.defaultCompiler.compile(graph, config, env)

    assertTrue(
      Concurrent.executeAndWait(
        () => env.execute(),
        () => sink.getRecordCount == 2,
        Duration.ofSeconds(10)))

    assertEquals(records.sorted, sink.getValues.sorted)
  }

  @Test
  def test_FlinkCompiler_CompileFromInstanceJson_WithSingleExternalStreamWithListSourceAndMemorySink_AddsSourceRecordsToSink(): Unit = {
    val stream = Stream.of[IntRecord]

    val graph = new StreamGraph(stream)

    val config = new application.ApplicationConfiguration()
    val records = List(IntRecord(1), IntRecord(2))
    config.setSource(stream, new application.sources.ListDataSource(records))

    val sink = new application.sinks.SingletonMemorySink[IntRecord]
    config.addSink(stream, sink)

    val app = new application.Application(graph)
    val instance = new application.ApplicationInstance(app, config)
    val json = instance.toJsonString

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    FlinkCompiler.defaultCompiler.compileFromInstanceJson(json, env)

    assertTrue(
      Concurrent.executeAndWait(
        () => env.execute(),
        () => sink.getRecordCount == 2,
        Duration.ofSeconds(10)))

    assertEquals(records.sorted, sink.getValues.sorted)
  }

  @Test
  def test_FlinkCompiler_Compile_WithExternalStreamMappedTwice_AddsSinkOnceAndUsesSameInputForEachMap(): Unit = {
    val input = Stream.of[IntRecord].withName("input")
    val map1 = input.map(i => i).withName("map1")
    val map2 = input.map(i => i).withName("map2")

    val graph = new StreamGraph(map1, map2)
    assertEquals(3, graph.getStreams.size)

    val config = new FlinkApplicationConfiguration()
    config.setListSource(input)

    val env = getTestExecutionEnvironment
    FlinkCompiler.defaultCompiler.compile(graph, config, env)
    assertEquals(1, env.getStreamGraph.getSourceIDs.size())

    val nodes = env.getStreamGraph.getStreamNodes
    assertEquals(1, nodes.asScala.count(_.getOperatorName == "Source: input"))
    assertEquals(1, nodes.asScala.count(_.getOperatorName == "Map [input] -> [map1]"))
    assertEquals(1, nodes.asScala.count(_.getOperatorName == "Map [input] -> [map2]"))
    assertEquals(3, nodes.asScala.count(!_.getOperatorName.contains("Lineage")))
  }

  @Test
  def test_FlinkCompiler_Compile_WithTwoIdenticalExternalStreamsMappedOnceEach_AddsSinkTwiceAndUsesDifferentInputForEachMap(): Unit = {
    val input1 = Stream.of[IntRecord].withName("input1")
    val input2 = Stream.of[IntRecord].withName("input2")
    val map1 = input1.map(i => i).withName("map1")
    val map2 = input2.map(i => i).withName("map2")

    val graph = new StreamGraph(map1, map2)
    assertEquals(4, graph.getStreams.size)

    val config = new FlinkApplicationConfiguration()
    config.setListSource(input1)
    config.setListSource(input2)

    val env = getTestExecutionEnvironment
    FlinkCompiler.defaultCompiler.compile(graph, config, env)
    assertEquals(2, env.getStreamGraph.getSourceIDs.size())

    val nodes = env.getStreamGraph.getStreamNodes
    assertEquals(1, nodes.asScala.count(_.getOperatorName == "Source: input1"))
    assertEquals(1, nodes.asScala.count(_.getOperatorName == "Source: input2"))
    assertEquals(1, nodes.asScala.count(_.getOperatorName == "Map [input1] -> [map1]"))
    assertEquals(1, nodes.asScala.count(_.getOperatorName == "Map [input2] -> [map2]"))
    assertEquals(4, nodes.asScala.count(!_.getOperatorName.contains("Lineage")))
  }
}
