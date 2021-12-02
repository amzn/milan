package com.amazon.milan.compiler.scala.event

import com.amazon.milan.Id
import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.compiler.scala.testing._
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang._
import com.amazon.milan.lang.aggregation._
import org.junit.Assert._
import org.junit.Test


object TestEventAppGroupBy {
  case class ListOfRecordsRecord(recordId: String, items: Array[ThreeIntRecord])

  def createListOfRecordsRecord(items: Iterable[ThreeIntRecord]): ListOfRecordsRecord = {
    ListOfRecordsRecord(Id.newId(), items.toArray)
  }
}


@Test
class TestEventAppGroupBy {
  @Test
  def test_EventAppGroupBy_ThenFlatMapWithSumBy_OutputsRunningSumForEachGroup(): Unit = {
    def sumByValue(group: Stream[KeyValueRecord]): Stream[KeyValueRecord] =
      group.sumBy(r => r.value, (r, sum) => KeyValueRecord(r.key, sum)).withId("sumBy")

    val input = Stream.of[KeyValueRecord].withId("input")
    val grouped = input.groupBy(r => r.key).withId("grouped")
    val output = grouped.flatMap((key, group) => sumByValue(group)).withId("output")

    val streams = StreamCollection.build(output)

    val config = new ApplicationConfiguration()
    val sink = config.addMemorySink(output)

    val target = EventAppTester.compile(streams, config)

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

    val streams = StreamCollection.build(output)

    val config = new ApplicationConfiguration()
    val sink = config.addMemorySink(output)

    val target = EventAppTester.compile(streams, config)

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

  @Test
  def test_EventAppGroupBy_ThenRecordWindowThenSelectSum_OutputsSumOfLatestPerGroup(): Unit = {
    val input = Stream.of[KeyValueRecord].withId("input")
    val grouped = input.groupBy(r => r.key).withId("grouped")
    val windowed = grouped.recordWindow(1).withId("windowed")
    val output = windowed.select(r => IntRecord(sum(r.value))).withId("output")

    val streams = StreamCollection.build(output)

    val config = new ApplicationConfiguration()
    val sink = config.addMemorySink(output)

    val target = EventAppTester.compile(streams, config)

    target.consume("input", KeyValueRecord(1, 1))
    assertEquals(IntRecord(1), sink.getValues.last)

    // The next record with the same key should evict the first one.
    target.consume("input", KeyValueRecord(1, 2))
    assertEquals(IntRecord(2), sink.getValues.last)

    // A record with a new key should add to the first one.
    target.consume("input", KeyValueRecord(2, 3))
    assertEquals(IntRecord(5), sink.getValues.last)

    // Another record with the second key should evict the first record with that key.
    target.consume("input", KeyValueRecord(2, 4))
    assertEquals(IntRecord(6), sink.getValues.last)
  }

  @Test
  def test_EventAppGroupBy_TheMaxByThenSelectSum_OutputsSumOfMaxPerGroup(): Unit = {
    def maxByValue(group: Stream[KeyValueRecord]): Stream[KeyValueRecord] =
      group.maxBy(r => r.value)

    val input = Stream.of[KeyValueRecord].withId("input")
    val grouped = input.groupBy(r => r.key).withId("grouped")
    val groupMapped = grouped.map((key, group) => maxByValue(group).withId("maxByValue")).withId("groupMapped")
    val windowed = groupMapped.recordWindow(1).withId("windowed")
    val output = windowed.select(r => KeyValueRecord(any(r.key), sum(r.value))).withId("output")

    val streams = StreamCollection.build(output)

    val config = new ApplicationConfiguration()
    val sink = config.addMemorySink(output)

    val target = EventAppTester.compile(streams, config)

    target.consume("input", KeyValueRecord(1, 1))
    assertEquals(KeyValueRecord(1, 1), sink.getValues.last)

    target.consume("input", KeyValueRecord(1, 3))
    assertEquals(KeyValueRecord(1, 3), sink.getValues.last)

    target.consume("input", KeyValueRecord(1, 2))
    assertEquals(KeyValueRecord(1, 3), sink.getValues.last)

    target.consume("input", KeyValueRecord(2, 1))
    assertEquals(KeyValueRecord(1, 4), sink.getValues.last)

    target.consume("input", KeyValueRecord(2, 5))
    assertEquals(KeyValueRecord(1, 8), sink.getValues.last)

    target.consume("input", KeyValueRecord(2, 4))
    assertEquals(KeyValueRecord(1, 8), sink.getValues.last)
  }

  @Test
  def test_EventAppGroupBy_ThenMaxByThenRecordWindowThenApply_SendsCorrectRecordsToApplyFunction(): Unit = {
    def maxGroupByB(items: Stream[ThreeIntRecord]): Stream[ThreeIntRecord] = {
      items.maxBy(r => r.b)
    }

    val input = Stream.of[ThreeIntRecord].withId("input")
    val grouped = input.groupBy(r => r.a).withId("grouped")
    val maxByB = grouped.map((k, g) => maxGroupByB(g)).withId("maxByB")
    val windowed = maxByB.recordWindow(1).withId("windowed")
    val output = windowed.apply(records => TestEventAppGroupBy.createListOfRecordsRecord(records)).withId("output")

    val streams = StreamCollection.build(output)

    val config = new ApplicationConfiguration()
    val sink = config.addMemorySink(output)

    val target = EventAppTester.compile(streams, config)

    val item111 = ThreeIntRecord(1, 1, 1)
    target.consume("input", item111)
    assertEquals(List(item111), sink.getValues.last.items.toList)

    val item122 = ThreeIntRecord(1, 2, 2)
    target.consume("input", item122)
    assertEquals(List(item122), sink.getValues.last.items.toList)

    val item231 = ThreeIntRecord(2, 3, 1)
    target.consume("input", item231)
    assertEquals(List(item122, item231), sink.getValues.last.items.toList)

    val item211 = ThreeIntRecord(2, 1, 1)
    target.consume("input", item211)
    assertEquals(List(item122, item231), sink.getValues.last.items.toList)
  }

  @Test
  def test_EventAppGroupBy_ThenMaxByThenSelectFieldsWithSumAndCount_OutputsExpectedValues(): Unit = {
    def maxByC(items: Stream[FourIntRecord]): Stream[FourIntRecord] = {
      items.maxBy(r => r.c)
    }

    def aggregateGroup(items: Stream[FourIntRecord]): Stream[(Int, Int, Long)] = {
      items
        .groupBy(r => r.b)
        .map((k, g) => maxByC(g))
        .recordWindow(1)
        .select(f => fields(
          field("key", any(f.a)),
          field("sum", sum(f.c)),
          field("count", count())
        )).withId("aggregated")
    }

    val input = Stream.of[FourIntRecord].withId("input")
    val grouped = input.groupBy(r => r.a).withId("grouped")
    val output = grouped.flatMap((k, g) => aggregateGroup(g)).withId("output")

    val streams = StreamCollection.build(output)

    val config = new ApplicationConfiguration()
    val sink = config.addMemorySink(output)

    val target = EventAppTester.compile(streams, config)

    target.consume("input", FourIntRecord(1, 1, 1, 1))
    assertEquals((1, 1, 1L), sink.getValues.last)

    target.consume("input", FourIntRecord(1, 1, 2, 2))
    assertEquals((1, 2, 1L), sink.getValues.last)
  }
}
