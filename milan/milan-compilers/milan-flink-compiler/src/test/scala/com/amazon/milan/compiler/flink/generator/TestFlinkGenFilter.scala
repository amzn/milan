package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.compiler.flink.testing.{IntRecord, TestApplicationExecutor, TwoIntRecord}
import com.amazon.milan.lang._
import com.amazon.milan.testing.applications._
import org.junit.Assert.assertEquals
import org.junit.Test


@Test
class TestFlinkGenFilter {
  @Test
  def test_FlinkGenFilter_WithFilterOnObjectStream_OutputsExpectedRecords(): Unit = {
    val stream = Stream.of[IntRecord]
    val filtered = stream.where(r => r.i == 3)

    val graph = new StreamGraph(filtered)

    val config = new ApplicationConfiguration()
    config.setListSource(stream, IntRecord(1), IntRecord(2), IntRecord(3), IntRecord(4))

    val results = TestApplicationExecutor.executeApplication(graph, config, 60, filtered)

    val actualOutput = results.getRecords(filtered)
    assertEquals(List(IntRecord(3)), actualOutput)
  }

  @Test
  def test_FlinkGenFilter_WithFilterOnTupleStream_OutputsExpectedRecords(): Unit = {
    val stream = Stream.of[TwoIntRecord]
    val tupleStream = stream.map(r => fields(field("a", r.a), field("b", r.b)))
    val filtered = tupleStream.where { case (a, b) => a == b }

    val graph = new StreamGraph(filtered)

    val config = new ApplicationConfiguration()
    config.setListSource(stream, TwoIntRecord(1, 2), TwoIntRecord(2, 2))

    val results = TestApplicationExecutor.executeApplication(graph, config, 60, filtered)

    val actualOutput = results.getRecords(filtered)
    assertEquals(List((2, 2)), actualOutput)
  }
}
