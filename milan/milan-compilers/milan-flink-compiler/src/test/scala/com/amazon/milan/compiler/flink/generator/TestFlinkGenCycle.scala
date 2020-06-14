package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.compiler.flink.testing.{IntRecord, TestApplicationExecutor}
import com.amazon.milan.lang._
import com.amazon.milan.testing.applications._
import org.junit.Assert._
import org.junit.Test


@Test
class TestFlinkGenCycle {
  @Test
  def test_FlinkGenCycle_WithLoopThatAddsOneEachTimeAndStopsAtTen_OutputsTenRecords(): Unit = {
    val input = Stream.of[IntRecord]
    val cycle = input.beginCycle()
    val addOne = cycle.map(r => IntRecord(r.i + 1))
    val output = addOne.where(r => r.i <= 10)

    cycle.closeCycle(output)

    val config = new ApplicationConfiguration
    config.setListSource(input, true, IntRecord(1))

    val graph = new StreamGraph(output)

    val results = TestApplicationExecutor.executeApplication(
      graph,
      config,
      60,
      intermediateResults => intermediateResults.getRecords(output).length < 9,
      output)

    val outputRecords = results.getRecords(output).sortBy(_.i)
    val expectedOutputRecords = List.tabulate(9)(i => IntRecord(i + 2))

    assertEquals(expectedOutputRecords, outputRecords)
  }
}
