package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.application.sources.ListDataSource
import com.amazon.milan.compiler.flink.testing.{IntRecord, TestApplicationExecutor, Tuple3Record, TwoIntRecord}
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang._
import com.amazon.milan.lang.aggregation.sum
import com.amazon.milan.testing.applications._
import org.junit.Assert._
import org.junit.Test


object TestFlinkGenGroupBy {
  def sumByF2(records: Iterable[Tuple3Record[Int, Int, Int]]): IntRecord =
    IntRecord(records.map(_.f2).sum)
}

@Test
class TestFlinkGenGroupBy {
  @Test
  def test_FlinkGenGroupBy_WithSelectSumAsRecord_OutputsSumOfInputs(): Unit = {
    val input = Stream.of[TwoIntRecord].withName("input")
    val grouped = input.groupBy(r => r.a)
    val output = grouped.select((key: Int, r: TwoIntRecord) => IntRecord(sum(r.a + r.b))).withName("output")

    val streams = StreamCollection.build(output)

    val config = new ApplicationConfiguration()
    config.setListSource(input, TwoIntRecord(1, 1), TwoIntRecord(2, 2), TwoIntRecord(1, 3), TwoIntRecord(2, 4), TwoIntRecord(3, 5))

    val results = TestApplicationExecutor.executeApplication(streams, config, 60, output)

    val expectedOutputRecords = List(IntRecord(1 + 1 + 1 + 3), IntRecord(2 + 2 + 2 + 4), IntRecord(3 + 5))
    val actualOutputRecords = results.getRecords(output)
    assertTrue(expectedOutputRecords.forall(actualOutputRecords.contains))
  }

  @Test
  def test_FlinkGenGroupBy_WithSelectSumAsField_OutputsSumOfInputs(): Unit = {
    val input = Stream.of[TwoIntRecord]
    val grouped = input.groupBy(r => r.a)
    val output = grouped.select((key, r) => fields(field("key", key), field("total", sum(r.b))))

    val streams = StreamCollection.build(output)

    val inputRecords = List(TwoIntRecord(1, 1), TwoIntRecord(2, 2), TwoIntRecord(1, 3), TwoIntRecord(2, 4), TwoIntRecord(3, 5))
    val config = new ApplicationConfiguration()
    config.setSource(input, new ListDataSource(inputRecords))

    val results = TestApplicationExecutor.executeApplication(streams, config, 60, output)

    val expectedOutputRecords = List((1, 4), (2, 6), (3, 5))
    val actualOutputRecords = results.getRecords(output)

    assertTrue(expectedOutputRecords.forall(actualOutputRecords.contains))
  }

  @Test
  def test_FlinkGenGroupBy_ThenMapWithMaxBy_ThenWindow_ThenApply(): Unit = {
    val input = Stream.of[Tuple3Record[Int, Int, Int]]

    def maxByF1(stream: Stream[Tuple3Record[Int, Int, Int]]): Stream[Tuple3Record[Int, Int, Int]] = {
      stream.maxBy(r => r.f1)
    }

    val output =
      input
        .groupBy(r => r.f0).withId("groupByF0")
        .map((key, group) => maxByF1(group).withId("maxByF1"))
        .recordWindow(1).withId("recordWindow")
        .apply(records => TestFlinkGenGroupBy.sumByF2(records)).withId("applyWindow")
        .last().withId("last")

    def record(a: Int, b: Int, c: Int): Tuple3Record[Int, Int, Int] =
      Tuple3Record[Int, Int, Int](a, b, c)

    // For each value of f0, pick the record with the highest f1.
    // Sum up f2 for all of those records.
    val testData = List(
      record(1, 1, 3),
      record(1, 2, 1), // this one is included in the final sum
      record(2, 1, 40),
      record(2, 3, 20), // this one is included in the final sum
      record(3, 1, 100),
      record(3, 2, 200) // this one is included in the final sum
    )

    val streams = StreamCollection.build(output)
    val config = new ApplicationConfiguration
    config.setListSource(input, testData: _*)

    val results = TestApplicationExecutor.executeApplication(
      streams,
      config,
      60,
      output)

    val outputRecords = results.getRecords(output)

    val expectedLastRecord = IntRecord(221)
    assertEquals(expectedLastRecord, outputRecords.last)
  }
}
