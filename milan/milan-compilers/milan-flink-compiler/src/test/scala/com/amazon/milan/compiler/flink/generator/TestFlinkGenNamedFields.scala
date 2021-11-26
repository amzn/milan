package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.compiler.flink.testing.{IntRecord, TestApplicationExecutor}
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang._
import com.amazon.milan.testing.applications._
import org.junit.Assert._
import org.junit.Test


@Test
class TestFlinkGenNamedFields {
  @Test
  def test_FlinkGenNamedFields_MapWithNamedFieldOutput_OutputsTupleObjects(): Unit = {
    val input = Stream.of[IntRecord]
    val output = input.map(r => fields(
      field("a", r.i + 1),
      field("b", r.i + 2),
      field("c", r.i.toString)
    ))

    val streams = StreamCollection.build(output)
    val config = new ApplicationConfiguration
    config.setListSource(input, IntRecord(1))

    val results = TestApplicationExecutor.executeApplication(
      streams,
      config,
      20,
      output)

    val outputRecords = results.getRecords(output)
    val expectedOutputRecords = List((2, 3, "1"))

    assertEquals(expectedOutputRecords, outputRecords)
  }
}
