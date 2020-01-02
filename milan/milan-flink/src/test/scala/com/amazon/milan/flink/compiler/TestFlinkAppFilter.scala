package com.amazon.milan.flink.compiler

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.application.sinks.SingletonMemorySink
import com.amazon.milan.flink.testing._
import com.amazon.milan.flink.testutil._
import com.amazon.milan.lang._
import com.amazon.milan.testing.applications._
import org.junit.Assert._
import org.junit.Test


@Test
class TestFlinkAppFilter {
  @Test
  def test_FlinkAppFilter_WithFilterOnObjectStream_OutputsExpectedRecords(): Unit = {
    val stream = Stream.of[IntRecord]
    val filtered = stream.where(r => r.i == 3)

    val graph = new StreamGraph(filtered)

    val config = new ApplicationConfiguration()
    config.setListSource(stream, IntRecord(1), IntRecord(2), IntRecord(3), IntRecord(4))

    val sink = new SingletonMemorySink[IntRecord]()
    config.addSink(filtered, sink)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount > 0, 1)

    assertEquals(List(IntRecord(3)), sink.getValues)
  }

  @Test
  def test_FlinkAppFilter_WithFilterOnTupleStream_OutputsExpectedRecords(): Unit = {
    val stream = Stream.of[TwoIntRecord]
    val tupleStream = stream.map(
      ((r: TwoIntRecord) => r.a) as "a",
      ((r: TwoIntRecord) => r.b) as "b")
    val filtered = tupleStream.where { case (a, b) => a == b }

    val graph = new StreamGraph(filtered)

    val config = new ApplicationConfiguration()
    config.setListSource(stream, TwoIntRecord(1, 2), TwoIntRecord(2, 2))

    val sink = new SingletonMemorySink[(Int, Int)]()
    config.addSink(filtered, sink)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount > 0, 1)

    assertEquals(List((2, 2)), sink.getValues)
  }

  @Test
  def test_FlinkAppFilter_WithFilterOperationParameterizedByFunctionArgument_OutputsExpectedRecords(): Unit = {
    def createGraph(threshold: Int): StreamGraph = {
      val input = Stream.of[IntRecord].withId("input")
      val output = input.where(r => r.i > threshold).withId("output")
      new StreamGraph(output)
    }

    val graph = createGraph(5)

    val config = new ApplicationConfiguration()
    config.setListSource("input", IntRecord(3), IntRecord(5), IntRecord(7), IntRecord(9))

    val sink = config.addMemorySink[IntRecord]("output")

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 2, 1)
    assertTrue(sink.getValues.forall(_.i > 5))
  }
}
