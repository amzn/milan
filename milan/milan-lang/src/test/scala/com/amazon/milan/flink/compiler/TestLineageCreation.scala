package com.amazon.milan.flink.compiler

import java.time.Duration

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.flink.application.FlinkApplicationConfiguration
import com.amazon.milan.flink.application.sinks.FlinkSingletonMemorySink
import com.amazon.milan.flink.testing._
import com.amazon.milan.flink.testutil._
import com.amazon.milan.lang._
import com.amazon.milan.test.{IntKeyValueRecord, IntRecord, KeyValueRecord}
import com.amazon.milan.testing.LineageAnalyzer
import com.amazon.milan.testing.applications._
import com.amazon.milan.types.RecordPointer
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.Assert._
import org.junit.Test

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, duration}
import scala.language.postfixOps


@Test
class TestLineageCreation {
  @Test
  def test_LineageCreation_WithMapToRecord_WithOneInputRecord_OutputsOneLineageRecordWithCorrectIds(): Unit = {
    val stream = Stream.of[IntRecord]
    val mapped = stream.map(r => IntRecord(r.i + 1))

    val graph = new StreamGraph(mapped)
    val config = new ApplicationConfiguration()

    val inputRecord = IntRecord(1)
    config.setListSource(stream, inputRecord)

    val sink = config.addMemorySink(mapped)
    val lineageSink = config.addMemoryLineageSink()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    compileFromSerialized(graph, config, env)

    env.executeThenWaitFor(() => lineageSink.getRecordCount == 1, 10)

    assertEquals(sink.getValues.head.recordId, lineageSink.getValues.head.subjectRecordId)
    assertEquals(stream.streamId, lineageSink.getValues.head.sourceRecords.head.streamId)
    assertEquals(inputRecord.recordId, lineageSink.getValues.head.sourceRecords.head.recordId)
  }

  @Test
  def test_LineageCreation_WithFullJoin_WithOneInputRecordOnEachStream_OutputsALineageRecordWithCorrectIds(): Unit = {
    val leftStream = Stream.of[IntKeyValueRecord]
    val rightStream = Stream.of[IntKeyValueRecord]
    val output =
      leftStream
        .fullJoin(rightStream)
        .where((l, r) => l.key == r.key && l != null && r != null)
        .select((l, r) => IntRecord(l.value + r.value))

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()
    val leftRecord = IntKeyValueRecord(1, 2)
    val rightRecord = IntKeyValueRecord(1, 3)
    config.setListSource(leftStream, leftRecord)
    config.setListSource(rightStream, rightRecord)

    val lineageSink = config.addMemoryLineageSink()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    compileFromSerialized(graph, config, env)

    val leftRecordPointer = RecordPointer(leftStream.streamId, leftRecord.recordId)
    val rightRecordPointer = RecordPointer(rightStream.streamId, rightRecord.recordId)
    env.executeThenWaitFor(
      () => lineageSink.getValues.exists(r =>
        r.sourceRecords.contains(leftRecordPointer) && r.sourceRecords.contains(rightRecordPointer)),
      10)
  }

  @Test
  def test_LineageCreation_WithLeftJoin_WithOneInputRecordOnEachStream_OutputsALineageRecordWithCorrectIds(): Unit = {
    val leftStream = Stream.of[IntKeyValueRecord]
    val rightStream = Stream.of[IntKeyValueRecord]
    val output =
      leftStream
        .leftJoin(rightStream)
        .where((l, r) => l.key == r.key && r != null)
        .select((l, r) => IntRecord(l.value + r.value))

    val graph = new StreamGraph(output)

    val config = new FlinkApplicationConfiguration()
    val leftRecord = IntKeyValueRecord(1, 2)
    val rightRecord = IntKeyValueRecord(1, 3)

    val leftSource = config.setMemorySource(leftStream, Seq(leftRecord), stopRunningWhenEmpty = true)
    val rightSource = config.setMemorySource(rightStream, Seq(rightRecord), stopRunningWhenEmpty = true)

    // We have to wait for the right source to send its record, then wait a bit longer to make sure the record
    // has been processed by the join operation.
    leftSource.waitFor(rightSource.awaitEmpty.thenWaitFor(Duration.ofMillis(300)), Duration.ofSeconds(10))

    val lineageSink = config.addMemoryLineageSink()

    val env = getTestExecutionEnvironment
    FlinkCompiler.defaultCompiler.compile(graph, config, env)

    val futureResult = env.executeUntilAsync(() => lineageSink.getRecordCount == 1, 10)

    Await.result(futureResult, duration.Duration(10, duration.SECONDS))

    val leftRecordPointer = RecordPointer(leftStream.streamId, leftRecord.recordId)
    val rightRecordPointer = RecordPointer(rightStream.streamId, rightRecord.recordId)
    val lineageRecord = lineageSink.getValues.head

    assertTrue(lineageRecord.sourceRecords.contains(leftRecordPointer))
    assertTrue(lineageRecord.sourceRecords.contains(rightRecordPointer))
  }

  @Test
  def test_LineageCreation_WithFiveSequentialJoinsAndOneInputRecordOnEachStream_OutputsLineageRecordsWithCorrectLineage(): Unit = {
    // Create the input streams.
    val dataStreams = Array.tabulate(5)(i => Stream.of[KeyValueRecord].withId(s"input$i"))

    // Apply a series of joins.
    val join1 = dataStreams(0).fullJoin(dataStreams(1)).where((l, r) => l.key == r.key && l != null && r != null).select((l, r) => l).withId("join1")
    val join2 = join1.fullJoin(dataStreams(2)).where((l, r) => l.key == r.key && l != null && r != null).select((l, r) => l).withId("join2")
    val join3 = join2.fullJoin(dataStreams(3)).where((l, r) => l.key == r.key && l != null && r != null).select((l, r) => l).withId("join3")
    val join4 = join3.fullJoin(dataStreams(4)).where((l, r) => l.key == r.key && l != null && r != null).select((l, r) => l).withId("join4")

    val graph = new StreamGraph(join4)

    // Generate input data.
    val inputRecords = Array.tabulate(5) { i => KeyValueRecord("key", i.toString) }

    // Connect the input streams to the input data.
    val config = new FlinkApplicationConfiguration()

    dataStreams.zip(inputRecords)
      .foreach { case (stream, record) => config.setMemorySource(stream, Seq(record), stopRunningWhenEmpty = true) }

    val outputSink = config.addMemorySink(join4)
    val lineageSink = config.addMemoryLineageSink()

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    FlinkCompiler.defaultCompiler.compile(graph, config, env)

    env.executeThenWaitFor(() => lineageSink.getRecordCount >= 4, 10)

    val lineageRecords = lineageSink.getValues
    val analyzer = new LineageAnalyzer(lineageRecords)

    // There should be a lineage record that has input0 and input1 as it's source.
    val lineageRecordJoin1 = analyzer.getImmediateDescendants("input0", inputRecords(0).recordId).head
    assertTrue(analyzer.isDescendedFrom(lineageRecordJoin1, "input1", inputRecords(1).recordId))

    // There should be a lineage record that has input2 and join1 as it's source.
    val lineageRecordJoin2 = analyzer.getImmediateDescendants("input2", inputRecords(2).recordId).head
    assertTrue(analyzer.isDescendedFrom(lineageRecordJoin2, "join1", lineageRecordJoin1.subjectRecordId))

    // There should be a lineage record that has input3 and join2 as it's source.
    val lineageRecordJoin3 = analyzer.getImmediateDescendants("input3", inputRecords(3).recordId).head
    assertTrue(analyzer.isDescendedFrom(lineageRecordJoin3, "join2", lineageRecordJoin2.subjectRecordId))

    // There should be a lineage record that has input4 and join3 as it's source.
    val lineageRecordJoin4 = analyzer.getImmediateDescendants("input4", inputRecords(4).recordId).head
    assertTrue(analyzer.isDescendedFrom(lineageRecordJoin4, "join3", lineageRecordJoin3.subjectRecordId))

    val outputRecord = outputSink.getValues.head
    assertEquals(outputRecord.recordId, lineageRecordJoin4.subjectRecordId)

    // The output record should be descended from all of the input records.
    assertTrue(analyzer.isDescendedFrom("join4", outputRecord.recordId, "input0", inputRecords(0).recordId))
    assertTrue(analyzer.isDescendedFrom("join4", outputRecord.recordId, "input1", inputRecords(1).recordId))
    assertTrue(analyzer.isDescendedFrom("join4", outputRecord.recordId, "input2", inputRecords(2).recordId))
    assertTrue(analyzer.isDescendedFrom("join4", outputRecord.recordId, "input3", inputRecords(3).recordId))
    assertTrue(analyzer.isDescendedFrom("join4", outputRecord.recordId, "input4", inputRecords(4).recordId))
  }

  @Test
  def test_LineageCreation_WithFiveParallelJoinsAndOneInputRecordOnEachStream_OutputsLineageRecordsWithCorrectLineage(): Unit = {
    // Create the input streams.
    val dataStreams = Array.tabulate(5)(i => Stream.of[KeyValueRecord].withId(s"input$i"))

    // Apply a series of joins.
    val join1 = dataStreams(0).fullJoin(dataStreams(1)).where((l, r) => l.key == r.key && l != null && r != null).select((l, r) => l).withId("join1")
    val join2 = join1.fullJoin(dataStreams(2)).where((l, r) => l.key == r.key && l != null && r != null).select((l, r) => l).withId("join2")
    val join3 = join1.fullJoin(dataStreams(3)).where((l, r) => l.key == r.key && l != null && r != null).select((l, r) => l).withId("join3")
    val join4 = join1.fullJoin(dataStreams(4)).where((l, r) => l.key == r.key && l != null && r != null).select((l, r) => l).withId("join4")

    val config = new FlinkApplicationConfiguration()

    // Generate input data.
    val inputRecords = Array.tabulate(5) { i => new KeyValueRecord("key", i.toString) }

    // Connect the input streams to the input data.
    dataStreams.zip(inputRecords).foreach { case (stream, record) => config.setListSource(stream, record) }

    val graph = new StreamGraph(join2, join3, join4)

    val outputSink = FlinkSingletonMemorySink.create[KeyValueRecord]

    config.addSink(join2, outputSink)
    config.addSink(join3, outputSink)
    config.addSink(join4, outputSink)

    val lineageSink = config.addMemoryLineageSink()

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    FlinkCompiler.defaultCompiler.compile(graph, config, env)

    env.executeThenWaitFor(() => lineageSink.getRecordCount >= 4, 10)

    val lineageRecords = lineageSink.getValues
    val analyzer = new LineageAnalyzer(lineageRecords)

    // There should be a lineage record that has input0 and input1 as it's source.
    val lineageRecordJoin1 = analyzer.getImmediateDescendants("input0", inputRecords(0).recordId).head
    assertTrue(analyzer.isDescendedFrom(lineageRecordJoin1, "input1", inputRecords(1).recordId))

    // There should be a lineage record that has input2 and join1 as it's source.
    val lineageRecordJoin2 = analyzer.getImmediateDescendants("input2", inputRecords(2).recordId).head
    assertTrue(analyzer.isDescendedFrom(lineageRecordJoin2, "join1", lineageRecordJoin1.subjectRecordId))

    // There should be a lineage record that has input3 and join1 as it's source.
    val lineageRecordJoin3 = analyzer.getImmediateDescendants("input3", inputRecords(3).recordId).head
    assertTrue(analyzer.isDescendedFrom(lineageRecordJoin3, "join1", lineageRecordJoin1.subjectRecordId))

    // There should be a lineage record that has input4 and join1 as it's source.
    val lineageRecordJoin4 = analyzer.getImmediateDescendants("input4", inputRecords(4).recordId).head
    assertTrue(analyzer.isDescendedFrom(lineageRecordJoin4, "join1", lineageRecordJoin1.subjectRecordId))

    val outputRecords = outputSink.getValues

    // Make sure there is a lineage record for every output record.
    outputRecords.foreach(or => assertTrue(lineageRecords.exists(_.subjectRecordId == or.recordId)))
  }
}
