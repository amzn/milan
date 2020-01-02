package com.amazon.milan.flink.compiler

import java.time.{Duration, Instant}

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.flink.testing._
import com.amazon.milan.flink.testutil._
import com.amazon.milan.lang._
import com.amazon.milan.lang.aggregation._
import com.amazon.milan.testing.applications._
import org.junit.Assert.assertEquals
import org.junit.Test


@Test
class TestFlinkAppWindow {
  @Test
  def test_FlinkAppWindow_TumblingWindow_WithRecordOutput_WithFiveInputRecordsInTwoDifferentWindows_OutputsExpectedAggregateValues(): Unit = {
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

    val sink = config.addMemorySink(output)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 5, 1)
  }

  @Test
  def test_FlinkAppWindow_TumblingWindow_WithFieldOutput_WithFiveInputRecordsInFiveDifferentWindows_OutputsFiveRecordsWithExpectedKeys(): Unit = {
    val input = Stream.of[DateIntRecord]
    val windowed = input.tumblingWindow(r => r.dateTime, Duration.ofMinutes(1), Duration.ZERO)
    val output = windowed.select(((t: Instant, _: DateIntRecord) => t) as "windowStartTime")

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()
    val now = Instant.ofEpochMilli(0)
    config.setListSource(
      input,
      DateIntRecord(now, 1),
      DateIntRecord(now.plusSeconds(20), 5),
      DateIntRecord(now.plusSeconds(40), 2),
      DateIntRecord(now.plusSeconds(60), 7),
      DateIntRecord(now.plusSeconds(80), 3))

    val sink = config.addMemorySink(output)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 5, 10)

    val windowStartTimes = sink.getValues.map(_._1).toSet
    assertEquals(2, windowStartTimes.size)
  }

  @Test
  def test_FlinkAppWindow_SlidingWindow_WithFieldOutput_WithOneInputRecordInSixWindows_OutputsSixWindowsWithSameValue(): Unit = {
    val input = Stream.of[DateIntRecord]
    val windowed = input.slidingWindow(r => r.dateTime, Duration.ofHours(1), Duration.ofMinutes(10), Duration.ZERO)
    val output = windowed.select(
      ((key: Instant, _: DateIntRecord) => key) as "windowStart",
      ((_: Instant, r: DateIntRecord) => sum(r.i)) as "i")

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()

    // If we put the record at 55 minutes after the epoch, then it should fall into six different windows.
    val timestamp = Instant.ofEpochSecond(55 * 60)
    config.setListSource(input, DateIntRecord(timestamp, 1))

    val sink = config.addMemorySink(output)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 6, 1)

    val windowStartTimes = sink.getValues.map(_._1).toSet
    assertEquals(6, windowStartTimes.size)

    val distinctValues = sink.getValues.map(_._2).toSet
    assertEquals(Set(1), distinctValues)
  }

  @Test
  def test_FlinkAppWindow_SlidingWindow_WithFieldOutput_WithThreeInputRecordInSixWindows_OutputsSixWindowsWithSameValues(): Unit = {
    val input = Stream.of[DateIntRecord]
    val windowed = input.slidingWindow(r => r.dateTime, Duration.ofHours(1), Duration.ofMinutes(10), Duration.ZERO)
    val output = windowed.select(
      ((key: Instant, _: DateIntRecord) => key) as "windowStart",
      ((_: Instant, r: DateIntRecord) => sum(r.i)) as "i")

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()

    // If we put the record at 55 minutes after the epoch, then it should fall into six different windows.
    val timestamp = Instant.ofEpochSecond(55 * 60)
    config.setListSource(input, DateIntRecord(timestamp, 1), DateIntRecord(timestamp, 2), DateIntRecord(timestamp, 3))

    val sink = config.addMemorySink(output)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 18, 1)

    val windowStartTimes = sink.getValues.map(_._1).toSet
    assertEquals(6, windowStartTimes.size)

    val distinctWindowMaxValues = sink.getValues.groupBy(_._1).map(_._2.map(_._2)).map(_.max).toSet
    assertEquals(Set(6), distinctWindowMaxValues)
  }

  @Test
  def test_FlinkAppWindow_GroupBy_ThenTumblingWindow_WithRecordOutput_OutputsExpectedRecords(): Unit = {
    val input = Stream.of[DateKeyValueRecord].withId("input")
    val output = input.groupBy(r => r.key)
      .tumblingWindow(r => r.dateTime, Duration.ofSeconds(5), Duration.ZERO)
      .select((_, r) => IntRecord(sum(r.value)))

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()

    val now = Instant.now()
    val later = now.plusSeconds(5)
    config.setListSource(input, DateKeyValueRecord(now, 1, 1), DateKeyValueRecord(now, 2, 2), DateKeyValueRecord(later, 1, 3), DateKeyValueRecord(later, 2, 4))
    val sink = config.addMemorySink(output)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 4, 1)
  }

  @Test
  def test_FlinkAppWindow_GroupBy_ThenTumblingWindow_WithFieldOutput_OutputsExpectedRecords(): Unit = {
    val input = Stream.of[DateKeyValueRecord].withId("input")
    val output = input.groupBy(r => r.key)
      .tumblingWindow(r => r.dateTime, Duration.ofSeconds(5), Duration.ZERO)
      .select(((_: Instant, r: DateKeyValueRecord) => sum(r.value)) as "sum")

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()

    val now = Instant.now()
    val later = now.plusSeconds(5)
    config.setListSource(input, DateKeyValueRecord(now, 1, 1), DateKeyValueRecord(now, 2, 2), DateKeyValueRecord(later, 1, 3), DateKeyValueRecord(later, 2, 4))
    val sink = config.addMemorySink(output)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 4, 1)
  }
}
