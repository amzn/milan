package com.amazon.milan.flink.application.sinks

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.application.sinks.KinesisDataSink
import com.amazon.milan.application.sources.ListDataSource
import com.amazon.milan.flink.testing._
import com.amazon.milan.flink.testutil.IntRecord
import com.amazon.milan.lang.{Stream, StreamGraph}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.Assert._
import org.junit.Test


@Test
class TestFlinkKinesisDataSink {
  @Test
  def test_FlinkKinesisDataSink_GetSinkFunction_WithQueueLimit_ReturnsNonNullSinkFunction(): Unit = {
    val sink = new FlinkKinesisDataSink[IntRecord]("kinesisStream", "region")
      .withQueueLimit(500)
    val sinkFunction = sink.getSinkFunction

    assertFalse(sinkFunction == null)
  }

  @Test
  def test_FlinkKinesisDataSink_InFlinkApp_WithQueueLimit_CompilesWithoutError(): Unit = {
    val inputStream = Stream.of[IntRecord]
    val source = new ListDataSource(List(IntRecord(4)))
    val graph = new StreamGraph(inputStream)
    val sink = new KinesisDataSink[IntRecord]("kinesisStream", "region", Some(500))

    val config = new ApplicationConfiguration()
    config.setSource(inputStream, source)
    config.addSink(inputStream, sink)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    compileFromSerialized(graph, config, env)
  }

  @Test
  def test_FlinkKinesisDataSink_CreateSinkFunction_WithoutQueueLimit_ReturnsNonNullSinkFunction(): Unit = {
    val sink = new FlinkKinesisDataSink[IntRecord]("kinesisStream", "region")
    val sinkFunction = sink.getSinkFunction

    assertFalse(sinkFunction == null)
  }

  @Test
  def test_FlinkKinesisDataSink_InFlinkApp_WithoutQueueLimit_CompilesWithoutError(): Unit = {
    val inputStream = Stream.of[IntRecord]
    val source = new ListDataSource(List(IntRecord(4)))
    val graph = new StreamGraph(inputStream)
    val sink = new KinesisDataSink[IntRecord]("kinesisStream", "region", Some(500))

    val config = new ApplicationConfiguration()
    config.setSource(inputStream, source)
    config.addSink(inputStream, sink)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    compileFromSerialized(graph, config, env)
  }
}
