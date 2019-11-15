package com.amazon.milan.flink.compiler

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.application.sinks.SingletonMemorySink
import com.amazon.milan.application.sources.ListDataSource
import com.amazon.milan.flink.testing._
import com.amazon.milan.flink.testutil._
import com.amazon.milan.lang._
import com.amazon.milan.lang.aggregation._
import com.amazon.milan.test._
import com.amazon.milan.testing.applications._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.Assert._
import org.junit.Test


@Test
class TestFlinkAppGroupBy {
  @Test
  def test_FlinkAppGroupBy_WithSelectSumAsRecord_OutputsSumOfInputs(): Unit = {
    val input = Stream.of[TwoIntRecord]
    val grouped = input.groupBy(r => r.a)
    val output = grouped.select((key: Int, r: TwoIntRecord) => IntRecord(sum(r.a + r.b)))

    val graph = new StreamGraph(output)

    val inputRecords = List(TwoIntRecord(1, 1), TwoIntRecord(2, 2), TwoIntRecord(1, 3), TwoIntRecord(2, 4), TwoIntRecord(3, 5))
    val config = new ApplicationConfiguration()
    config.setSource(input, new ListDataSource(inputRecords))
    val sink = new SingletonMemorySink[IntRecord]
    config.addSink(output, sink)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    compileFromSerialized(graph, config, env)

    val expectedOutputRecords = List(IntRecord(1 + 1 + 1 + 3), IntRecord(2 + 2 + 2 + 4), IntRecord(3 + 5))

    env.executeThenWaitFor(() => expectedOutputRecords.forall(sink.getValues.contains), 1)
  }

  @Test
  def test_FlinkAppGroupBy_WithSelectSumAsField_OutputsSumOfInputs(): Unit = {
    val input = Stream.of[TwoIntRecord]
    val grouped = input.groupBy(r => r.a)
    val output = grouped.select(
      ((key: Int, _: TwoIntRecord) => key) as "key",
      ((_: Int, r: TwoIntRecord) => sum(r.b)) as "total")

    val graph = new StreamGraph(output)

    val inputRecords = List(TwoIntRecord(1, 1), TwoIntRecord(2, 2), TwoIntRecord(1, 3), TwoIntRecord(2, 4), TwoIntRecord(3, 5))
    val config = new ApplicationConfiguration()
    config.setSource(input, new ListDataSource(inputRecords))
    val sink = new SingletonMemorySink[(Int, Int)]
    config.addSink(output, sink)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    compileFromSerialized(graph, config, env)

    val expectedOutputRecords = List((1, 4), (2, 6), (3, 5))

    env.executeThenWaitFor(() => expectedOutputRecords.forall(sink.getValues.contains), 1)
  }

  @Test
  def test_FlinkAppGroupBy_ThenJoinWithTuple_ProducesOutputRecords(): Unit = {
    val leftInput = Stream.of[IntKeyValueRecord]

    val left = leftInput.groupBy(_.key).select((_, r) => any(r))

    val rightInput = Stream.of[IntKeyValueRecord]
    val right = rightInput.map(((r: IntKeyValueRecord) => r) as "a")

    val output = left.fullJoin(right).where((l: IntKeyValueRecord, r: Tuple1[IntKeyValueRecord]) => l != null && r != null && (r match {
      case Tuple1(a) => l.value == a.key
    }))
      .select(
        ((l: IntKeyValueRecord, _: Tuple1[IntKeyValueRecord]) => l) as "left",
        ((_: IntKeyValueRecord, r: Tuple1[IntKeyValueRecord]) => r match {
          case Tuple1(a) => a
        }) as "right")

    val graph = new StreamGraph(output)
    val config = new ApplicationConfiguration()

    config.setListSource(leftInput, IntKeyValueRecord(1, 1), IntKeyValueRecord(1, 2))
    config.setListSource(rightInput, IntKeyValueRecord(1, 3))

    val sink = config.addMemorySink(output)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount > 0, 1)
  }

  @Test
  def test_FlinkAppGroupBy_ThenMaxBy_ProducesStreamOfMaxValuesPerGroup(): Unit = {
    val input = Stream.of[Tuple3Record[Int, Int, Int]]
    val output = input.groupBy(r => r.f0).maxBy(r => r.f1)

    val graph = new StreamGraph(output)
    val config = new ApplicationConfiguration()

    def record(a: Int, b: Int, c: Int): Tuple3Record[Int, Int, Int] = new Tuple3Record(a, b, c)

    config.setListSource(input, record(1, 1, 3), record(2, 2, 1), record(1, 3, 1), record(2, 1, 4))

    val sink = config.addMemorySink(output)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 4, 1)

    val outputRecords = sink.getValues

    val lastGroup1Record = outputRecords.filter(_.f0 == 1).last
    assertEquals(record(1, 3, 1), lastGroup1Record)

    val lastGroup2Record = outputRecords.filter(_.f0 == 2).last
    assertEquals(record(2, 2, 1), lastGroup2Record)
  }
}
