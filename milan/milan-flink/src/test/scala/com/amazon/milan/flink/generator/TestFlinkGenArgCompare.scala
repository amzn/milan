package com.amazon.milan.flink.generator

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.flink.testing._
import com.amazon.milan.flink.testutil._
import com.amazon.milan.lang._
import com.amazon.milan.testing.applications._
import org.junit.Assert._
import org.junit.Test


@Test
class TestFlinkGenArgCompare {
  @Test
  def test_FlinkGenArgCompare_MaxBy_OfNonKeyedStream_OutputsLargestRecord(): Unit = {
    val input = Stream.of[IntKeyValueRecord]
    val output = input.maxBy(_.value)

    val graph = new StreamGraph(output)

    // We don't have fine control over the ordering of the data as it flows through and out of the Flink application,
    // but we can be fairly sure that our maxBy operation shouldn't output *all* of the input records if the max record
    // appears early on, so that's what we'll test for.

    val inputRecords =
      List(IntKeyValueRecord(1, 1), IntKeyValueRecord(2, 5)) ++
        List.tabulate(100)(_ => IntKeyValueRecord(3, 3))

    val config = new ApplicationConfiguration
    config.setListSource(input, inputRecords: _*)

    val results = TestApplicationExecutor.executeApplication(graph, config, 60, output)

    val outputRecords = results.getRecords(output)

    // We should prevent at least some of the input records from getting to the output.
    assertTrue(outputRecords.length < inputRecords.length)

    // The max record should appear in the output.
    val maxOutputRecord = outputRecords.maxBy(_.value)
    assertEquals(IntKeyValueRecord(2, 5), maxOutputRecord)
  }

  @Test
  def test_FlinkGenArgCompare_MaxBy_InFlatMapOfGroupBy_OutputsLargestAsLastRecordPerKey(): Unit = {
    val input = Stream.of[IntKeyValueRecord]

    def maxByValue(stream: Stream[IntKeyValueRecord]): Stream[IntKeyValueRecord] = {
      stream.maxBy(r => r.value).last()
    }

    val output = input.groupBy(r => r.key).flatMap((key, group) => maxByValue(group))

    val graph = new StreamGraph(output)

    val data = generateIntKeyValueRecords(100, 5, 100)

    val config = new ApplicationConfiguration
    config.setListSource(input, data: _*)

    val results = TestApplicationExecutor.executeApplication(graph, config, 60, output)

    val outputRecords = results.getRecords(output)
    val lastOutputRecordPerKey = outputRecords.groupBy(_.key).map { case (key, group) => key -> group.last }
    val expectedLastOutputRecordPerKey = data.groupBy(_.key).map { case (key, group) => key -> group.maxBy(_.value) }
    assertEquals(expectedLastOutputRecordPerKey, lastOutputRecordPerKey)
  }
}
