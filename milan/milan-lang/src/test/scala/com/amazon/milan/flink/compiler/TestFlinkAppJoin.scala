package com.amazon.milan.flink.compiler

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.application.sinks.SingletonMemorySink
import com.amazon.milan.application.sources.ListDataSource
import com.amazon.milan.flink.testing._
import com.amazon.milan.flink.testutil._
import com.amazon.milan.lang._
import com.amazon.milan.test._
import com.amazon.milan.testing.applications._
import com.amazon.milan.typeutil._
import org.junit.Assert._
import org.junit.Test


@Test
class TestFlinkAppJoin {
  @Test
  def test_FlinkAppJoin_WithFullJoinWithConditionAndSelectToRecord_OutputsExpectedJoinedRecords(): Unit = {
    val left = Stream.of[KeyValueRecord]
    val right = Stream.of[KeyValueRecord]

    val joined = left.fullJoin(right)
    val conditioned = joined.where((l, r) => l.key == r.key)
    val selected = conditioned.select((l, r) => TestFlinkApplications.combineRecords(l, r))

    val graph = new StreamGraph(selected)

    val leftInputRecords = List(new KeyValueRecord("1", "l1"), new KeyValueRecord("2", "l2"))
    val rightInputRecords = List(new KeyValueRecord("1", "r1"), new KeyValueRecord("2", "r2"))
    val config = new ApplicationConfiguration
    config.setSource(left, new ListDataSource(leftInputRecords))
    config.setSource(right, new ListDataSource(rightInputRecords))
    val sink = new SingletonMemorySink[KeyValueRecord]
    config.addSink(selected, sink)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 4, 1)

    val outputs = sink.getValues
    assertTrue(outputs.contains(new KeyValueRecord("1", "l1r1")))
    assertTrue(outputs.contains(new KeyValueRecord("2", "l2r2")))
  }

  @Test
  def test_FlinkAppJoin_WithFullJoinWithConditionAndSelectToFields_OutputsExpectedJoinedRecords(): Unit = {
    val left = Stream.of[KeyValueRecord]
    val right = Stream.of[KeyValueRecord]

    val joined = left.fullJoin(right)
    val conditioned = joined.where((l, r) => l.key == r.key)
    val selected = conditioned.select(
      ((l: KeyValueRecord, r: KeyValueRecord) => if (l == null) "" else l.value) as "leftValue",
      ((l: KeyValueRecord, r: KeyValueRecord) => if (r == null) "" else r.value) as "rightValue")

    val graph = new StreamGraph(selected)

    val leftInputRecords = List(new KeyValueRecord("1", "l1"), new KeyValueRecord("2", "l2"))
    val rightInputRecords = List(new KeyValueRecord("1", "r1"), new KeyValueRecord("2", "r2"))
    val config = new ApplicationConfiguration
    config.setSource(left, new ListDataSource(leftInputRecords))
    config.setSource(right, new ListDataSource(rightInputRecords))

    val sink = new SingletonMemorySink[(String, String)]
    config.addSink(selected, sink)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 4, 1)

    val outputs = sink.getValues
    assertTrue(outputs.contains(("l1", "r1")))
    assertTrue(outputs.contains(("l2", "r2")))
  }

  @Test
  def test_FlinkAppJoin_WithFullJoinWithPostConditionAndSelectToFields_OutputsExpectedJoinedRecords(): Unit = {
    val left = Stream.of[KeyValueRecord]
    val right = Stream.of[KeyValueRecord]

    val joined = left.fullJoin(right)

    // The null check is a post-condition because it's a condition on the state of the join, not on input records.
    val conditioned = joined.where((l, r) => l.key == r.key && l != null && r != null)
    val selected = conditioned.select(
      ((l: KeyValueRecord, r: KeyValueRecord) => l.value) as "leftValue",
      ((l: KeyValueRecord, r: KeyValueRecord) => r.value) as "rightValue")

    val graph = new StreamGraph(selected)

    val leftInputRecords = List(new KeyValueRecord("1", "l1"), new KeyValueRecord("2", "l2"))
    val rightInputRecords = List(new KeyValueRecord("1", "r1"), new KeyValueRecord("2", "r2"))
    val config = new ApplicationConfiguration
    config.setSource(left, new ListDataSource(leftInputRecords))
    config.setSource(right, new ListDataSource(rightInputRecords))

    val sink = new SingletonMemorySink[(String, String)]
    config.addSink(selected, sink)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    // If this doesn't crash then the post-conditions are succeeding.
    env.executeThenWaitFor(() => sink.getRecordCount >= 2, 1)

    val outputs = sink.getValues
    assertTrue(outputs.contains(("l1", "r1")))
    assertTrue(outputs.contains(("l2", "r2")))
  }

  @Test
  def test_FlinkAppJoin_WithLeftJoinWithConditionAndSelectToRecord_OutputsExpectedJoinedRecords(): Unit = {
    val left = Stream.of[KeyValueRecord]
    val right = Stream.of[KeyValueRecord]

    val joined = left.leftJoin(right)
    val conditioned = joined.where((l, r) => l.key == r.key)
    val selected = conditioned.select((l, r) => TestFlinkApplications.combineRecords(l, r))

    val graph = new StreamGraph(selected)

    val leftInputRecords = List(new KeyValueRecord("1", "l1"), new KeyValueRecord("2", "l2"))
    val rightInputRecords = List(new KeyValueRecord("1", "r1"), new KeyValueRecord("2", "r2"))
    val config = new ApplicationConfiguration
    config.setSource(left, new ListDataSource(leftInputRecords))
    config.setSource(right, new ListDataSource(rightInputRecords))
    val sink = new SingletonMemorySink[KeyValueRecord]
    config.addSink(selected, sink)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 2, 1)

    val outputs = sink.getValues
    assertTrue(outputs.exists(_.key == "1"))
    assertTrue(outputs.exists(_.key == "2"))
  }

  @Test
  def test_FlinkAppJoin_WithFullJoinWithPostConditionOppositeOfJoinCondition_OutputsNoRecords(): Unit = {
    val left = Stream.of[KeyValueRecord]
    val right = Stream.of[KeyValueRecord]

    val joined = left.fullJoin(right)

    // The != will end up as a post-condition that filters everything.
    val conditioned = joined.where((l, r) => l.key == r.key && l != null && r != null && l.key != r.key)
    val selected = conditioned.select((l, r) => TestFlinkApplications.combineRecords(l, r))

    val graph = new StreamGraph(selected)

    val leftInputRecords = List(new KeyValueRecord("1", "l1"), new KeyValueRecord("2", "l2"))
    val rightInputRecords = List(new KeyValueRecord("1", "r1"), new KeyValueRecord("2", "r2"))
    val config = new ApplicationConfiguration
    config.setSource(left, new ListDataSource(leftInputRecords))
    config.setSource(right, new ListDataSource(rightInputRecords))
    val sink = new SingletonMemorySink[KeyValueRecord]
    config.addSink(selected, sink)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.execute()
    Thread.sleep(100)

    val outputs = sink.getValues
    assertTrue(outputs.isEmpty)
  }

  @Test
  def test_FlinkAppJoin_JoinSelectOfThreeStreamsWithAllThreeInOutput_OutputsExpectedJoinedRecords(): Unit = {
    val streamA = Stream.of[KeyValueRecord].withId("a")
    val streamB = Stream.of[KeyValueRecord].withId("b")
    val streamC = Stream.of[KeyValueRecord].withId("c")

    val streamAB = streamA.fullJoin(streamB)
      .where((a, b) => a.key == b.key && a != null && b != null)
      .select(
        ((a: KeyValueRecord, _: KeyValueRecord) => a) as "a",
        ((_: KeyValueRecord, b: KeyValueRecord) => b) as "b")
      .withId("ab")

    val streamABC = streamC.fullJoin(streamAB)
      .where((c: KeyValueRecord, ab: (KeyValueRecord, KeyValueRecord)) => ab != null && (ab match {
        case (a, b) => a.key == c.key && a != null && b != null && c != null
      }))
      .select(
        ((c: KeyValueRecord, ab: (KeyValueRecord, KeyValueRecord)) => ab match {
          case (a, b) => a
        }) as "a",
        ((c: KeyValueRecord, ab: (KeyValueRecord, KeyValueRecord)) => ab match {
          case (a, b) => b
        }) as "b",
        ((c: KeyValueRecord, ab: (KeyValueRecord, KeyValueRecord)) => ab match {
          case (a, b) => c
        }) as "c")
      .withId("abc")

    val graph = new StreamGraph(streamABC)

    val config = new ApplicationConfiguration()
    config.setListSource(streamA, KeyValueRecord("key", "a"))
    config.setListSource(streamB, KeyValueRecord("key", "b"))
    config.setListSource(streamC, KeyValueRecord("key", "c"))

    val sink = config.addMemorySink(streamABC)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 1, 1)

    val outputRecord = sink.getValues.head
    assertEquals((KeyValueRecord("key", "a"), KeyValueRecord("key", "b"), KeyValueRecord("key", "c")), outputRecord)
  }

  @Test
  def test_FlinkAppJoin_ThenSelectAll_WithTwoTupleStreams_OutputsExpectedJoinedRecords(): Unit = {
    val leftInput = Stream.of[IntKeyValueRecord].withId("leftInput")
    val left = leftInput.map(((r: IntKeyValueRecord) => r.key) as "a", ((r: IntKeyValueRecord) => r.value) as "b").withId("left")

    val rightInput = Stream.of[IntStringRecord].withId("rightInput")
    val right = rightInput.map(((r: IntStringRecord) => r.i) as "c", ((r: IntStringRecord) => r.s) as "d").withId("right")

    val joined = left.fullJoin(right).where((l, r) =>
      l != null &&
        r != null && (
        l match {
          case (a, _) => r match {
            case (c, _) => a == c
          }
        }))
    val output = joined.selectAll().withId("output")

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()
    config.setListSource(leftInput, IntKeyValueRecord(1, 1), IntKeyValueRecord(2, 2))
    config.setListSource(rightInput, IntStringRecord(1, "1"), IntStringRecord(2, "2"))

    // Something goes wrong with the implicit TypeDescriptor parameter to addMemorySink.
    // The TypeDescriptor created by the compiler has the correct type of (Int, Int, Int, String),
    // but the one passed to addMemorySink has type (String, String, String, String).
    // Explicitly passing the TypeDescriptor for the record type of the output stream works around the problem.
    val sinkType = output.getRecordType
    val sink = config.addMemorySink(output)(sinkType)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 2, 1)

    val outputRecords = sink.getValues
    assertTrue(outputRecords.contains((1, 1, 1, "1")))
    assertTrue(outputRecords.contains((2, 2, 2, "2")))
  }
}
