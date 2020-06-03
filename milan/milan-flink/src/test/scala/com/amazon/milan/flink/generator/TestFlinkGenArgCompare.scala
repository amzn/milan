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
  def test_FlinkGenArgCompare_MaxBy_OfNonKeyedStream_OutputsLargestAsLastRecord(): Unit = {
    val input = Stream.of[IntKeyValueRecord]
    val output = input.maxBy(_.value)

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration
    config.setListSource(input, IntKeyValueRecord(1, 1), IntKeyValueRecord(2, 5), IntKeyValueRecord(3, 3))

    val results = TestApplicationExecutor.executeApplication(graph, config, 60, output)

    val outputRecords = results.getRecords(output)
    val lastOutputRecord = outputRecords.last
    assertEquals(IntKeyValueRecord(2, 5), lastOutputRecord)
  }

  @Test
  def test_FlinkGenArgCompare_MaxBy_InFlatMapOfGroupBy_OutputsLargestAsLastRecordPerKey(): Unit = {
    val input = Stream.of[IntKeyValueRecord]

    def maxByValue(stream: Stream[IntKeyValueRecord]): Stream[IntKeyValueRecord] = {
      stream.maxBy(r => r.value)
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
