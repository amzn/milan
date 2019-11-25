package com.amazon.milan.flink.compiler

import java.time.{Duration, Instant}

import com.amazon.milan.flink.application.FlinkApplicationConfiguration
import com.amazon.milan.flink.testing._
import com.amazon.milan.flink.testutil._
import com.amazon.milan.lang.{Stream, StreamGraph}
import com.amazon.milan.test.{DateKeyValueRecord, IntKeyValueRecord, IntRecord}
import org.apache.flink.api.scala._
import org.junit.Assert._
import org.junit.Test

import scala.concurrent.ExecutionContext.Implicits.global


object TestFlinkAppWindowedJoin {
  def getMaxValue(records: Iterable[DateKeyValueRecord]): Int = {
    records.map(_.value).max
  }

  def getClosestValue(i: Int, candidates: Iterable[DateKeyValueRecord]): Int = {
    if (candidates.isEmpty) {
      Int.MaxValue
    }
    else {
      candidates.map(_.value).minBy(c => Math.abs(c - i))
    }
  }
}

@Test
class TestFlinkAppWindowedJoin {
  @Test
  def test_FlinkAppWindowedJoin_UsingLatestByThenApply_Compiles(): Unit = {
    val left = Stream.of[IntRecord]
    val right = Stream.of[DateKeyValueRecord]

    val rightLatest = right.latestBy(r => r.dateTime, r => r.key)

    val output =
      left.leftJoin(rightLatest)
        .apply((leftRecord, rightRecords) => IntKeyValueRecord(leftRecord.i, TestFlinkAppWindowedJoin.getMaxValue(rightRecords)))

    val graph = new StreamGraph(output)

    val config = new FlinkApplicationConfiguration()

    val now = Instant.now()
    val later = now.plusSeconds(1)

    val rightData = List(DateKeyValueRecord(now, 1, 3), DateKeyValueRecord(later, 1, 2))
    val rightSource = config.setMemorySource(right, rightData, stopRunningWhenEmpty = true)

    val leftData = List(IntRecord(1))
    val leftSource = config.setMemorySource(left, leftData, stopRunningWhenEmpty = true)
    leftSource.waitFor(rightSource.awaitEmpty.thenWaitFor(Duration.ofMillis(300)), Duration.ofSeconds(10))

    val env = getTestExecutionEnvironment

    FlinkCompiler.defaultCompiler.compile(graph, config, env)
  }

  @Test
  def test_FlinkAppWindowedJoin_DoesNotConsiderOldValues(): Unit = {
    val left = Stream.of[IntKeyValueRecord]
    val right = Stream.of[DateKeyValueRecord]

    val rightLatest = right.latestBy(r => r.dateTime, r => r.key)

    val output =
      left.leftJoin(rightLatest)
        .apply((leftRecord, rightRecords) => IntKeyValueRecord(leftRecord.key, TestFlinkAppWindowedJoin.getClosestValue(leftRecord.value, rightRecords)))

    val graph = new StreamGraph(output)

    val config = new FlinkApplicationConfiguration()

    val now = Instant.now()
    val before = now.plusSeconds(-1)

    val rightData = Seq(
      DateKeyValueRecord(before, 1, 1),
      DateKeyValueRecord(before, 2, 10),
      DateKeyValueRecord(now, 3, 15),
      DateKeyValueRecord(now, 2, 20))
    val rightSource = config.setMemorySource(right, rightData, stopRunningWhenEmpty = true)

    val leftData = List(IntKeyValueRecord(5, 10))
    val leftSource = config.setMemorySource(left, leftData, stopRunningWhenEmpty = true)
    leftSource.waitFor(rightSource.awaitEmpty.thenWaitFor(Duration.ofMillis(300)), Duration.ofSeconds(10))

    val sink = config.addMemorySink(output)

    val env = getTestExecutionEnvironment
    FlinkCompiler.defaultCompiler.compile(graph, config, env)

    env.executeThenWaitFor(() => sink.getRecordCount == 1, 1)

    val outputRecord = sink.getValues.head
    assertEquals(IntKeyValueRecord(5, 15), outputRecord)
  }
}
