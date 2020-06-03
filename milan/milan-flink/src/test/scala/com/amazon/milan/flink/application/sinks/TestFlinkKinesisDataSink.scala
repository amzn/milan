package com.amazon.milan.flink.application.sinks

import com.amazon.milan.flink.testing.IntRecord
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
  def test_FlinkKinesisDataSink_CreateSinkFunction_WithoutQueueLimit_ReturnsNonNullSinkFunction(): Unit = {
    val sink = new FlinkKinesisDataSink[IntRecord]("kinesisStream", "region")
    val sinkFunction = sink.getSinkFunction

    assertFalse(sinkFunction == null)
  }
}
