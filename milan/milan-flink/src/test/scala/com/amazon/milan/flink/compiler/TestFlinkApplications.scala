package com.amazon.milan.flink.compiler

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.application.sinks.SingletonMemorySink
import com.amazon.milan.application.sources.ListDataSource
import com.amazon.milan.flink.testing._
import com.amazon.milan.flink.testutil._
import com.amazon.milan.lang._
import com.amazon.milan.testing.applications._
import com.amazon.milan.typeutil._
import org.junit.Assert._
import org.junit.Test


object TestFlinkApplications {
  def mapIntToString(r: IntRecord) = StringRecord(r.i.toString)

  def combineRecords(left: KeyValueRecord, right: KeyValueRecord): KeyValueRecord = {
    if (left == null) {
      new KeyValueRecord(right.key, right.value)
    }
    else if (right == null) {
      new KeyValueRecord(left.key, left.value)
    }
    else {
      new KeyValueRecord(left.key, left.value + right.value)
    }
  }
}

import com.amazon.milan.flink.compiler.TestFlinkApplications._


@Test
class TestFlinkApplications {
  @Test
  def test_FlinkApplication_WithOneMappedRecordStream_OutputsMappedRecords(): Unit = {
    val input = Stream.of[IntRecord]
    val mapped = input.map(r => mapIntToString(r))

    val graph = new StreamGraph(mapped)

    val inputRecords = List(IntRecord(1), IntRecord(2), IntRecord(3))
    val config = new ApplicationConfiguration
    config.setSource(input, new ListDataSource(inputRecords))
    val sink = new SingletonMemorySink[StringRecord]
    config.addSink(mapped, sink)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 3, 1)

    val expectedOutputRecords = List(StringRecord("1"), StringRecord("2"), StringRecord("3"))

    // The output records can arrive in a different order due to parallelism, so we need to sort them to be able to
    // test for equality between the lists.
    val outputRecords = sink.getValues.sortBy(_.s)

    assertEquals(expectedOutputRecords, outputRecords)
  }

  @Test
  def test_FlinkApplication_WithOneMappedTupleStreamWithOneField_OutputsMappedRecords(): Unit = {
    val input = Stream.of[IntRecord]
    val mapped = input.map(((r: IntRecord) => r.i) as "i")

    val graph = new StreamGraph(mapped)

    val inputRecords = List(IntRecord(1), IntRecord(2), IntRecord(3))
    val config = new ApplicationConfiguration
    config.setSource(input, new ListDataSource(inputRecords))
    val sink = new SingletonMemorySink[Tuple1[Int]]
    config.addSink(mapped, sink)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 3, 1)

    val expectedOutputRecords = List(Tuple1(1), Tuple1(2), Tuple1(3))

    // The output records can arrive in a different order due to parallelism, so we need to sort them to be able to
    // test for equality between the lists.
    val outputRecords = sink.getValues.sortBy(_._1)

    assertEquals(expectedOutputRecords, outputRecords)
  }

  @Test
  def test_FlinkApplication_WithMappedTupleToRecordStream_OutputsMappedRecords(): Unit = {
    val input = Stream.of[IntRecord]
    val tupleStream = input.map(((r: IntRecord) => r.i) as "i")
    val outputStream = tupleStream.map(r => r match {
      case Tuple1(i) => IntRecord(i)
    })

    val graph = new StreamGraph(outputStream)

    val inputRecords = List(IntRecord(1), IntRecord(2), IntRecord(3))
    val config = new ApplicationConfiguration
    config.setSource(input, new ListDataSource(inputRecords))
    val sink = new SingletonMemorySink[IntRecord]
    config.addSink(outputStream, sink)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 3, 1)

    val outputRecords = sink.getValues.sortBy(_.i)
    assertEquals(inputRecords, outputRecords)
  }

  @Test
  def test_FlinkApplication_WithParameterizedFilter(): Unit = {
    val config = new ApplicationConfiguration()
    config.setListSource("input", IntRecord(1), IntRecord(2), IntRecord(3), IntRecord(4), IntRecord(5))
    config.addMemorySink[IntRecord]("output")

    val env = getTestExecutionEnvironment
  }

  @Test
  def test_FlinkApplication_WithTupleStreamAddField_OutputsRecordsWithAddedField(): Unit = {
    val input = Stream.of[IntRecord]
    val tuple = input.toField("i")
    val output = tuple.addField(((t: Tuple1[IntRecord]) => t match {
      case Tuple1(r) => r.i + 1
    }) as "plus_one")

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()
    config.setListSource(input, IntRecord(1))
    val sink = config.addMemorySink(output)(output.recordType)

    val env = getTestExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 1, 1)

    assertEquals((IntRecord(1), 2), sink.getValues.head)
  }
}
