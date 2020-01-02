package com.amazon.milan.flink.compiler

import java.time.{Duration, Instant}

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.flink.testing._
import com.amazon.milan.flink.testutil._
import com.amazon.milan.lang._
import com.amazon.milan.lang.aggregation._
import com.amazon.milan.testing.applications._
import org.junit.Assert._
import org.junit.Test


class TestFlinkAppUnique {
  @Test
  def test_FlinkAppUnique_OfGroupBy_WithSelectMaxAsRecord_WithDuplicateInputRecords_UsesOnlyLatestRecord(): Unit = {
    val input = Stream.of[Tuple3Record[Int, Int, Int]]
    val grouped = input.groupBy(r => r.f0)
    val unique = grouped.unique(r => r.f1)
    val output = unique.select((_: Int, r: Tuple3Record[Int, Int, Int]) => IntRecord(max(r.f2)))

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()
    val inputRecords = List(
      Tuple3Record(1, 1, 1),
      Tuple3Record(1, 1, 3),
      Tuple3Record(1, 1, 2),
    )
    config.setListSource(input, inputRecords: _*)

    val sink = config.addMemorySink(output)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == inputRecords.length, 1)

    val lastOutput = sink.getValues.last
    assertEquals(2, lastOutput.i)
  }

  @Test
  def test_FlinkAppUnique_OfGroupBy_WithSelectMaxAndSumAsFields_WithDuplicateInputRecords_UsesOnlyLatestRecord(): Unit = {
    val input = Stream.of[Tuple3Record[Int, Int, Int]]
    val grouped = input.groupBy(r => r.f0)
    val unique = grouped.unique(r => r.f1)
    val output = unique.select(
      ((_: Int, r: Tuple3Record[Int, Int, Int]) => max(r.f2)) as "max",
      ((_: Int, r: Tuple3Record[Int, Int, Int]) => sum(r.f2)) as "sum")

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()
    val inputRecords = List(
      Tuple3Record(1, 1, 1),
      Tuple3Record(1, 1, 3),
      Tuple3Record(1, 1, 2),
      Tuple3Record(1, 2, 5),
      Tuple3Record(1, 2, 1),
    )
    config.setListSource(input, inputRecords: _*)

    val sink = config.addMemorySink(output)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == inputRecords.length, 1)

    val lastOutput = sink.getValues.last
    val (lastMax, lastSum) = lastOutput
    assertEquals(2, lastMax)
    assertEquals(3, lastSum)
  }

  @Test
  def test_FlinkAppUnique_OfTumblingWindow_WithUniqueAndSelectMaxAsRecord_WithDuplicateInputRecords_UsesOnlyLatestRecord(): Unit = {
    val input = Stream.of[Tuple3Record[Instant, Int, Int]]
    val windowed = input.tumblingWindow(r => r.f0, Duration.ofMinutes(1), Duration.ZERO)
    val unique = windowed.unique(r => r.f1)
    val output = unique.select((_: Instant, r: Tuple3Record[Instant, Int, Int]) => IntRecord(max(r.f2)))

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()
    val now = Instant.now()

    // The input records have duplicate keys wrt to the uniqueness constraint, so even though the last record
    // has a smaller value than the previous record, it will replace it in the max calculation.
    val inputRecords = List(
      Tuple3Record(now, 1, 1),
      Tuple3Record(now, 1, 3),
      Tuple3Record(now, 1, 2),
    )
    config.setListSource(input, inputRecords: _*)

    val sink = config.addMemorySink(output)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == inputRecords.length, 1)

    val lastOutput = sink.getValues.last

    // TODO: this fails sometimes because the output records arrive out-of-order.
    // assertEquals(2, lastOutput.i)
  }

  @Test
  def test_FlinkAppUnique_OfTumblingWindow_WithSelectMaxAndSumAsFields_WithDuplicateInputRecords_UsesOnlyLatestRecords(): Unit = {
    val input = Stream.of[Tuple3Record[Instant, Int, Int]]
    val windowed = input.tumblingWindow(r => r.f0, Duration.ofMinutes(1), Duration.ZERO)
    val unique = windowed.unique(r => r.f1)
    val output = unique.select(
      ((t: Instant, _: Tuple3Record[Instant, Int, Int]) => t) as "time",
      ((_: Instant, r: Tuple3Record[Instant, Int, Int]) => max(r.f2)) as "max",
      ((_: Instant, r: Tuple3Record[Instant, Int, Int]) => sum(r.f2)) as "sum"
    )

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()

    val firstTime = Instant.now()
    val secondTime = firstTime.plus(Duration.ofSeconds(90))

    val inputRecords = List(
      Tuple3Record(firstTime, 1, 1),
      Tuple3Record(secondTime, 1, 6),
      Tuple3Record(firstTime, 1, 2),
      Tuple3Record(secondTime, 2, 5),
      Tuple3Record(firstTime, 2, 3),
      Tuple3Record(secondTime, 2, 4),
    )
    config.setListSource(input, inputRecords: _*)

    val sink = config.addMemorySink(output)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == inputRecords.length, 1)

    val outputs = sink.getValues

    val outputTimes = outputs.map(_._1).toSet
    val firstOutputTime = outputTimes.min
    val secondOutputTime = outputTimes.max

    assertTrue(outputs.exists { case (t, m, s) => t == firstOutputTime && m == 1 && s == 1 })
    assertTrue(outputs.exists { case (t, m, s) => t == firstOutputTime && m == 2 && s == 2 })
    assertTrue(outputs.exists { case (t, m, s) => t == firstOutputTime && m == 3 && s == 5 })

    assertTrue(outputs.exists { case (t, m, s) => t == secondOutputTime && m == 6 && s == 6 })
    assertTrue(outputs.exists { case (t, m, s) => t == secondOutputTime && m == 6 && s == 11 })
    assertTrue(outputs.exists { case (t, m, s) => t == secondOutputTime && m == 6 && s == 10 })

    // TODO: once sequence number is available in the output, add test for the final output for each group.
  }

  @Test
  def test_FlinkAppUnique_OfTumblingWindowOfGroupBy_WithSelectMinAsRecord_WithDuplicateInputRecords_ReturnsSumForEachGroupAndWindow(): Unit = {
    val input = Stream.of[Tuple4Record[Int, Instant, Int, Int]]
    val grouped = input.groupBy(r => r.f0)
    val windowed = grouped.tumblingWindow(r => r.f1, Duration.ofMinutes(5), Duration.ZERO)
    val unique = windowed.unique(r => r.f2)
    val output = unique.select((t, r) => Tuple3Record[Int, Instant, Int](any(r.f0), t, sum(r.f3)))

    val graph = new StreamGraph(output)
    val config = new ApplicationConfiguration()

    val time1 = Instant.now()
    val time2 = time1.plus(Duration.ofMinutes(5))

    def record(a: Int, b: Instant, c: Int, d: Int): Tuple4Record[Int, Instant, Int, Int] = Tuple4Record(a, b, c, d)

    config.setListSource(
      input,
      record(1, time1, 1, 30),
      record(1, time1, 1, 40),
      record(1, time2, 1, 50),
      record(1, time2, 1, 40),
      record(1, time1, 2, 20))

    val sink = config.addMemorySink(output)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 5, 1)

    val outputRecords = sink.getValues

    val windowTimes = outputRecords.map(_.f1).toSet
    assertEquals(2, windowTimes.size)

    val firstWindowTime = windowTimes.min
    val secondWindowTime = windowTimes.max

    val lastGroup1Time1 = outputRecords.filter(r => r.f0 == 1 && r.f1 == firstWindowTime).last
    assertEquals(60, lastGroup1Time1.f2)

    val lastGroup1Time2 = outputRecords.filter(r => r.f0 == 1 && r.f1 == secondWindowTime).last
    assertEquals(40, lastGroup1Time2.f2)
  }

  @Test
  def test_FlinkAppUnique_OfSlidingWindowOfGroupBy_WithSelectMeanAsField_RunsWithoutError(): Unit = {
    val input = Stream.of[Tuple4Record[Int, Instant, Int, Int]]
    val grouped = input.groupBy(r => r.f0)
    val windowed = grouped.tumblingWindow(r => r.f1, Duration.ofMinutes(5), Duration.ZERO)
    val unique = windowed.unique(r => r.f2)
    val output = unique.select(
      ((t: Instant, _: input.RecordType) => t) as "time",
      ((_: Instant, r: input.RecordType) => mean(r.f3)) as "mean")

    def record(a: Int, b: Instant, c: Int, d: Int): Tuple4Record[Int, Instant, Int, Int] = Tuple4Record(a, b, c, d)

    val graph = new StreamGraph(output)
    val config = new ApplicationConfiguration()

    val time1 = Instant.now()
    val time2 = time1.plus(Duration.ofMinutes(5))

    config.setListSource(
      input,
      record(1, time1, 1, 30),
      record(1, time1, 1, 40),
      record(1, time2, 1, 50),
      record(1, time2, 1, 40),
      record(1, time1, 2, 20))

    val sink = config.addMemorySink(output)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 5, 1)
  }
}
