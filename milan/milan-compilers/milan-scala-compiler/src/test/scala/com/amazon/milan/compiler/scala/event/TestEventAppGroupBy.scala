package com.amazon.milan.compiler.scala.event

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.compiler.scala.testing.KeyValueRecord
import com.amazon.milan.lang._
import org.junit.Assert._
import org.junit.Test


@Test
class TestEventAppGroupBy {
  @Test
  def test_EventAppGroupBy_ThenFlatMapWithSumBy_OutputsRunningSumForEachGroup(): Unit = {
    def sumByValue(group: Stream[KeyValueRecord]): Stream[KeyValueRecord] =
      group.sumBy(r => r.value, (r, sum) => KeyValueRecord(r.key, sum)).withId("sumBy")

    val input = Stream.of[KeyValueRecord].withId("input")
    val grouped = input.groupBy(r => r.key).withId("grouped")
    val output = grouped.flatMap((key, group) => sumByValue(group)).withId("output")

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()
    val sink = config.addMemorySink(output)

    val target = EventAppTester.compile(graph, config)

    target.consume("input", KeyValueRecord(1, 1))
    assertEquals(KeyValueRecord(1, 1), sink.getValues.last)

    target.consume("input", KeyValueRecord(1, 2))
    assertEquals(KeyValueRecord(1, 3), sink.getValues.last)

    target.consume("input", KeyValueRecord(2, 3))
    assertEquals(KeyValueRecord(2, 3), sink.getValues.last)

    target.consume("input", KeyValueRecord(2, 5))
    assertEquals(KeyValueRecord(2, 8), sink.getValues.last)
  }

  @Test
  def test_EventAppGroupBy_ThenFlatMapWithMaxBy_OutputsIncreasingValuesForEachGroup(): Unit = {
    def maxByValue(group: Stream[KeyValueRecord]): Stream[KeyValueRecord] =
      group.maxBy(r => r.value).withId("maxBy")

    val input = Stream.of[KeyValueRecord].withId("input")
    val grouped = input.groupBy(r => r.key).withId("grouped")
    val output = grouped.flatMap((key, group) => maxByValue(group)).withId("output")

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()
    val sink = config.addMemorySink(output)

    val target = EventAppTester.compile(graph, config)

    target.consume("input", KeyValueRecord(1, 1))
    assertEquals(KeyValueRecord(1, 1), sink.getValues.last)

    target.consume("input", KeyValueRecord(1, 3))
    assertEquals(KeyValueRecord(1, 3), sink.getValues.last)

    target.consume("input", KeyValueRecord(1, 2))
    assertEquals(KeyValueRecord(1, 3), sink.getValues.last)

    target.consume("input", KeyValueRecord(2, 8))
    assertEquals(KeyValueRecord(2, 8), sink.getValues.last)

    target.consume("input", KeyValueRecord(2, 3))
    assertEquals(KeyValueRecord(2, 8), sink.getValues.last)
  }
}
