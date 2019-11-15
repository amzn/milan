package com.amazon.milan.flink.compiler

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.flink.testing._
import com.amazon.milan.lang._
import com.amazon.milan.lang.aggregation._
import com.amazon.milan.test.{IntKeyValueRecord, KeyValueRecord, StringRecord}
import com.amazon.milan.testing.applications._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.StreamNode
import org.junit.Test


@Test
class TestOperatorNaming {
  @Test
  def test_OperatorNaming_Map_HasExpectedName(): Unit = {

    val stream = Stream.of[KeyValueRecord].withName("input")
    val mapped = stream.map(r => StringRecord(r.key)).withName("map")

    val expectedNames = List(
      "Map [input] -> [map]",
      "LineageSplit [Map [input] -> [map]]"
    )

    val graph = new StreamGraph(mapped)

    val config = new ApplicationConfiguration()
    config.setListSource(stream)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    compileFromSerialized(graph, config, env)

    val streamNodeNames = env.getStreamGraph.getStreamNodes.toArray().map(node => node.asInstanceOf[StreamNode].getOperatorName)

    expectedNames.foreach(expectedName =>
      assert(streamNodeNames.contains(expectedName),
        s"Name: '$expectedName' was not found in stream node names (${streamNodeNames.mkString(", ")})."))
  }

  @Test
  def test_OperatorNaming_LeftJoin_HasExpectedName(): Unit = {
    val left = Stream.of[KeyValueRecord].withName("left")
    val right = Stream.of[KeyValueRecord].withName("right")
    val joined = left.leftJoin(right).where((l, r) => l.key == r.key).select((l, r) => l).withName("joined")

    val expectedNames = List(
      "LeftEnrichmentJoin [left] with [right] -> [joined]",
      "LineageSplit [LeftEnrichmentJoin [left] with [right] -> [joined]]"
    )

    val graph = new StreamGraph(joined)
    val config = new ApplicationConfiguration()
    config.setListSource(left)
    config.setListSource(right)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    compileFromSerialized(graph, config, env)

    val streamNodeNames = env.getStreamGraph.getStreamNodes.toArray().map(node => node.asInstanceOf[StreamNode].getOperatorName)

    expectedNames.foreach(expectedName =>
      assert(streamNodeNames.contains(expectedName),
        s"Name: '$expectedName' was not found in stream node names (${streamNodeNames.mkString(", ")})."))
  }

  @Test
  def test_OperatorNaming_FullJoin_HasExpectedName(): Unit = {
    val left = Stream.of[KeyValueRecord].withName("left")
    val right = Stream.of[KeyValueRecord].withName("right")
    val joined = left.fullJoin(right).where((l, r) => l.key == r.key).select((l, r) => l).withName("joined")

    val expectedNames = List(
      "FullEnrichmentJoin [left] with [right] -> [joined]",
      "LineageSplit [FullEnrichmentJoin [left] with [right] -> [joined]]"
    )

    val graph = new StreamGraph(joined)
    val config = new ApplicationConfiguration()
    config.setListSource(left)
    config.setListSource(right)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    compileFromSerialized(graph, config, env)

    val streamNodeNames = env.getStreamGraph.getStreamNodes.toArray().map(node => node.asInstanceOf[StreamNode].getOperatorName)

    expectedNames.foreach(expectedName =>
      assert(streamNodeNames.contains(expectedName),
        s"Name: '$expectedName' was not found in stream node names (${streamNodeNames.mkString(", ")})."))
  }

  @Test
  def test_OperatorNaming_Filter_HasExpectedName(): Unit = {
    val stream = Stream.of[KeyValueRecord].withName("input")
    val filtered = stream.where(r => r.key == "foo")

    val expectedNames = List(
      "Filter [input]"
    )

    val graph = new StreamGraph(filtered)
    val config = new ApplicationConfiguration()
    config.setListSource(stream)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    compileFromSerialized(graph, config, env)

    val streamNodeNames = env.getStreamGraph.getStreamNodes.toArray().map(node => node.asInstanceOf[StreamNode].getOperatorName)

    expectedNames.foreach(expectedName =>
      assert(streamNodeNames.contains(expectedName),
        s"Name: '$expectedName' was not found in stream node names (${streamNodeNames.mkString(", ")})."))
  }

  @Test
  def test_OperatorNaming_Aggregate_HasExpectedName(): Unit = {
    val stream = Stream.of[IntKeyValueRecord].withName("input")
    val grouped = stream.groupBy(r => r.key)
    val output = grouped.select(((key: Int, r: IntKeyValueRecord) => sum(r.value)) as "sum").withName("output")

    val expectedNames = List(
      "AggregateSelect [output]"
    )

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()
    config.setListSource(stream)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    compileFromSerialized(graph, config, env)

    val streamNodeNames = env.getStreamGraph.getStreamNodes.toArray().map(node => node.asInstanceOf[StreamNode].getOperatorName)

    expectedNames.foreach(expectedName =>
      assert(streamNodeNames.contains(expectedName),
        s"Name: '$expectedName' was not found in stream node names (${streamNodeNames.mkString(", ")})."))
  }

  @Test
  def test_OperatorNaming_SourceAndSink_HaveExpectedNames(): Unit = {
    val stream = Stream.of[KeyValueRecord].withName("input")

    val graph = new StreamGraph(stream)

    val config = new ApplicationConfiguration()
    config.setListSource(stream)
    config.addMemorySink(stream)

    val expectedNames = List(
      "Source: input",
      "Sink: input"
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    compileFromSerialized(graph, config, env)

    val streamNodeNames = env.getStreamGraph.getStreamNodes.toArray().map(node => node.asInstanceOf[StreamNode].getOperatorName)

    expectedNames.foreach(expectedName =>
      assert(streamNodeNames.contains(expectedName),
        s"Name: '$expectedName' was not found in stream node names (${streamNodeNames.mkString(", ")})."))
  }
}
