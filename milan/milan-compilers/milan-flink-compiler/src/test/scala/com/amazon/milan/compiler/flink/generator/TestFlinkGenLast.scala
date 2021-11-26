package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.compiler.flink.testing._
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang._
import com.amazon.milan.testing.applications._
import org.junit.Assert._
import org.junit.Test

import scala.util.Random


@Test
class TestFlinkGenLast {
  @Test
  def test_FlinkGenLast_InFlatMapOfGroupBy_WithOneGroupKeyInInputRecords_OutputsOnlyLastInputRecordToOutput(): Unit = {
    val input = Stream.of[IntKeyValueRecord].withName("input")
    val grouped = input.groupBy(r => r.key)

    def maxByValueAndLast(stream: Stream[IntKeyValueRecord]): Stream[IntKeyValueRecord] =
      stream.maxBy(r => r.value).last()

    val output = grouped.flatMap((key, group) => maxByValueAndLast(group)).withName("output")

    val streams = StreamCollection.build(output)

    val config = new ApplicationConfiguration
    config.setListSource(input, IntKeyValueRecord(1, 1), IntKeyValueRecord(1, 3), IntKeyValueRecord(1, 2))

    // Keep running until we find records in the output file.
    val results = TestApplicationExecutor.executeApplication(
      streams,
      config,
      20,
      r => r.getRecords(output).isEmpty,
      output)

    val outputRecords = results.getRecords(output)
    assertEquals(List(IntKeyValueRecord(1, 3)), outputRecords)
  }

  @Test
  def test_FlinkGenLast_InFlatMapOfGroupBy_With10GroupKeysInInputRecords_With10RecordsPerGroupKey_OutputsOnlyLastRecordInInputForEachGroupKey(): Unit = {
    val input = Stream.of[IntKeyValueRecord].withName("input")
    val grouped = input.groupBy(r => r.key)

    def maxByValueAndLast(stream: Stream[IntKeyValueRecord]): Stream[IntKeyValueRecord] =
      stream.maxBy(r => r.value).last()

    val output = grouped.flatMap((key, group) => maxByValueAndLast(group)).withName("output")

    val streams = StreamCollection.build(output)

    val inputRecords = Random.shuffle(List.tabulate(10)(group => List.tabulate(10)(i => IntKeyValueRecord(group, i))).flatten)
    val config = new ApplicationConfiguration
    config.setListSource(input, inputRecords: _*)

    val results = TestApplicationExecutor.executeApplication(
      streams,
      config,
      20,
      r => r.getRecords(output).length < 10,
      output)

    val outputRecords = results.getRecords(output).sortBy(_.key)
    val expectedOutputRecords = List.tabulate(10)(i => inputRecords.filter(_.key == i).maxBy(_.value))
    assertEquals(expectedOutputRecords, outputRecords)
  }
}
