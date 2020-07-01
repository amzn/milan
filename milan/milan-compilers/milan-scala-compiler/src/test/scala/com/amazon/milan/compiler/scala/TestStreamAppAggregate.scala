package com.amazon.milan.compiler.scala

import com.amazon.milan.compiler.scala.testing.KeyValueRecord
import com.amazon.milan.lang
import org.junit.Assert._
import org.junit.Test


@Test
class TestStreamAppAggregate {
  @Test
  def test_StreamAppAggregate_GroupBy_WithMaxBy_OutputsMaxPerGroup(): Unit = {
    def maxByValue(group: lang.Stream[KeyValueRecord]): lang.Stream[KeyValueRecord] =
      group.maxBy(r => r.value)

    val input = lang.Stream.of[KeyValueRecord].withId("input")
    val group = input.groupBy(r => r.key).withId("group")
    val output = group.flatMap((key, group) => maxByValue(group)).withId("output")

    val compiledFunction = StreamAppTester.compile(input, output)

    val inputRecords = KeyValueRecord.generate(100, 5, 100)
    val outputRecords = compiledFunction(inputRecords.toStream).toList

    val lastOutputRecords = outputRecords.groupBy(_.key).map { case (_, g) => g.last }.toList.sortBy(_.key)
    val expectedLastOutputRecords = inputRecords.groupBy(_.key).map { case (_, g) => g.maxBy(_.value) }.toList.sortBy(_.key)
    assertEquals(expectedLastOutputRecords, lastOutputRecords)
  }

  @Test
  def test_StreamAppAggregate_GroupBy_WithSumBy_OutputsSumPerGroupd(): Unit = {
    def sumByValue(group: lang.Stream[KeyValueRecord]): lang.Stream[KeyValueRecord] =
      group.sumBy(r => r.value, (r, sum) => KeyValueRecord(r.key, sum))

    val input = lang.Stream.of[KeyValueRecord].withId("input")
    val group = input.groupBy(r => r.key).withId("group")
    val output = group.flatMap((key, group) => sumByValue(group)).withId("output")

    val compiledFunction = StreamAppTester.compile(input, output)

    val inputRecords = KeyValueRecord.generate(100, 5, 100)
    val outputRecords = compiledFunction(inputRecords.toStream).toList

    assertEquals(inputRecords.length, outputRecords.length)

    val lastOutputRecords = outputRecords.groupBy(_.key).map { case (_, g) => g.last }.toList.sortBy(_.key)
    val expectedLastOutputRecords = inputRecords.groupBy(_.key).map { case (k, g) => KeyValueRecord(k, g.map(_.value).sum) }.toList.sortBy(_.key)
    assertEquals(expectedLastOutputRecords, lastOutputRecords)
  }
}
