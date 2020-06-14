package com.amazon.milan.compiler.flink.generator

import java.time.{Duration, Instant}

import com.amazon.milan.Id
import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.compiler.flink.testing._
import com.amazon.milan.lang._
import com.amazon.milan.lang.aggregation._
import com.amazon.milan.testing.applications._
import org.junit.Assert._
import org.junit.Test


object TestFlinkGenWindow {

  class TestRecord(var recordId: String, var timeStamp: Instant, var key1: Int, var key2: Int, var i: Int) {
    def this() {
      this(Id.newId(), Instant.MIN, 0, 0, 0)
    }

    override def toString: String = s"(${this.timeStamp}, ${this.key1}, ${this.key2}, ${this.i})"

    override def equals(obj: Any): Boolean = obj match {
      case o: TestRecord => this.timeStamp.equals(o.timeStamp) && this.key1 == o.key1 && this.key2 == o.key2 && this.i == o.i
      case _ => false
    }
  }

  object TestRecord {
    def apply(timeStamp: Instant, key1: Int, key2: Int, i: Int): TestRecord =
      new TestRecord(Id.newId(), timeStamp, key1, key2, i)
  }

}

import com.amazon.milan.compiler.flink.generator.TestFlinkGenWindow._


@Test
class TestFlinkGenWindow {
  @Test
  def test_FlinkGenWindow_TumblingWindow_WithRecordOutput_WithFiveInputRecordsInTwoDifferentWindows_OutputsExpectedAggregateValues(): Unit = {
    val input = Stream.of[DateIntRecord]
    val windowed = input.tumblingWindow(r => r.dateTime, Duration.ofMinutes(1), Duration.ZERO)
    val output = windowed.select((_, r) => argmax(r.i, r))

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()
    val now = Instant.now()
    config.setListSource(
      input,
      DateIntRecord(now, 1),
      DateIntRecord(now.plusSeconds(20), 5),
      DateIntRecord(now.plusSeconds(40), 2),
      DateIntRecord(now.plusSeconds(60), 7),
      DateIntRecord(now.plusSeconds(80), 3))

    val results = TestApplicationExecutor.executeApplication(graph, config, 60, output)

    val outputRecords = results.getRecords(output)
    assertTrue(outputRecords.length >= 5)
  }

  @Test
  def test_FlinkGenWindow_SlidingWindow_WithFieldOutput_WithOneInputRecordInSixWindows_OutputsSixWindowsWithSameValue(): Unit = {
    val input = Stream.of[DateIntRecord]
    val windowed = input.slidingWindow(r => r.dateTime, Duration.ofHours(1), Duration.ofMinutes(10), Duration.ZERO)
    val output = windowed.select((key, r) => fields(
      field("windowStart", key),
      field("i", sum(r.i))
    ))

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()

    // If we put the record at 55 minutes after the epoch, then it should fall into six different windows.
    val timestamp = Instant.ofEpochSecond(55 * 60)
    config.setListSource(input, DateIntRecord(timestamp, 1))

    val results = TestApplicationExecutor.executeApplication(graph, config, 60, output)

    val outputRecords = results.getRecords(output)
    assertTrue(outputRecords.length >= 6)

    val windowStartTimes = outputRecords.map(_._1).toSet
    assertEquals(6, windowStartTimes.size)

    val distinctValues = outputRecords.map(_._2).toSet
    assertEquals(Set(1), distinctValues)
  }

  @Test
  def test_FlinkGenWindow_GroupByTuple_ThenTumblingWindow_ThenMaxBy_WithRecordsFromTwoTimeWindowsAndTwoGroups_ProducesLargestValuePerWindowAndGroup(): Unit = {
    val input = Stream.of[TestRecord].withId("input")

    def maxByTimeStampAndValue(stream: Stream[TestRecord]): Stream[TestRecord] =
      stream.maxBy(r => (r.timeStamp, r.i)).last()

    val output = input.groupBy(r => (r.key1, r.key2))
      .tumblingWindow(r => r.timeStamp, Duration.ofSeconds(5), Duration.ZERO)
      .flatMap((_, window) => maxByTimeStampAndValue(window))

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()

    val now = Instant.now()
    val later = now.plusSeconds(6)
    config.setListSource(
      input,
      TestRecord(now, 1, 1, 1),
      TestRecord(now, 1, 1, 5),
      TestRecord(now, 1, 1, 3),
      TestRecord(later, 1, 1, 12),
      TestRecord(later, 1, 1, 11),
      TestRecord(later, 1, 1, 10)
    )

    val results = TestApplicationExecutor.executeApplication(graph, config, 60, output)

    val outputRecords = results.getRecords(output)
    val lastRecordPerGroupAndWindow =
      outputRecords.groupBy(r => (r.timeStamp, r.key1, r.key2)).map {
        case (_, group) => group.last
      }.toList.sortBy(r => (r.timeStamp, r.key1, r.key2))

    val expectedLastRecords = List(
      TestRecord(now, 1, 1, 5),
      TestRecord(later, 1, 1, 12)
    )

    assertEquals(expectedLastRecords, lastRecordPerGroupAndWindow)
  }
}
