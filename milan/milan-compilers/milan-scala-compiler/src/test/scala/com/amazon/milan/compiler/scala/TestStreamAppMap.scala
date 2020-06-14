package com.amazon.milan.compiler.scala

import com.amazon.milan.compiler.scala.testing.{IntRecord, KeyValueRecord}
import com.amazon.milan.lang
import org.junit.Assert._
import org.junit.Test


@Test
class TestStreamAppMap {
  @Test
  def test_StreamAppMap_OfDataStream_MapsRecords(): Unit = {
    val inputStream = lang.Stream.of[IntRecord].withId("input")
    val outputStream = inputStream.map(r => IntRecord(r.i + 1)).withId("output")

    val compiledFunction = StreamAppTester.compile(outputStream)

    val output = compiledFunction(Stream(IntRecord(1), IntRecord(2))).toList
    assertEquals(List(IntRecord(2), IntRecord(3)), output)
  }

  @Test
  def test_StreamAppMap_ThenFlatMap_OfGroupedStream_MapsGroups(): Unit = {
    val input = lang.Stream.of[KeyValueRecord].withId("input")
    val grouped = input.groupBy(r => r.key)

    def addOne(s: lang.Stream[KeyValueRecord]): lang.Stream[KeyValueRecord] =
      s.map(r => KeyValueRecord(r.key, r.value + 1))

    def sumByValue(s: lang.Stream[KeyValueRecord]): lang.Stream[KeyValueRecord] =
      s.sumBy(r => r.value, (r, sum) => KeyValueRecord(r.key, sum))

    val mapped = grouped.map((_, group) => addOne(group)).withId("mapped")
    val output = mapped.flatMap((_, group) => sumByValue(group)).withId("output")

    val compiledFunction = StreamAppTester.compile(output)

    val inputRecords = KeyValueRecord.generate(100, 5, 10)
    val outputRecords = compiledFunction(inputRecords.toStream).toList

    assertEquals(inputRecords.length, outputRecords.length)

    val lastOutputRecords = outputRecords.groupBy(_.key).map { case (_, g) => g.last }.toList.sortBy(_.key)

    val expectedLastOutputRecords =
      inputRecords
        .groupBy(_.key)
        .map { case (k, g) => k -> g.map(r => KeyValueRecord(r.key, r.value + 1)) }
        .map { case (k, g) => KeyValueRecord(k, g.map(_.value).sum) }
        .toList
        .sortBy(_.key)

    assertEquals(expectedLastOutputRecords, lastOutputRecords)
  }
}
