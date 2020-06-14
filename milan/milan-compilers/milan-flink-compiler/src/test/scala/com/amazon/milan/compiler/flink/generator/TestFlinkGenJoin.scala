package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.application.sources.ListDataSource
import com.amazon.milan.compiler.flink.testing.{IntRecord, KeyValueRecord, StringRecord, TestApplicationExecutor}
import com.amazon.milan.lang._
import org.junit.Assert._
import org.junit.Test


object TestFlinkGenJoin {
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


@Test
class TestFlinkGenJoin {
  @Test
  def test_FlinkGenJoin_WithFullJoinWithConditionAndSelectToRecord_OutputsExpectedJoinedRecords(): Unit = {
    val left = Stream.of[KeyValueRecord]
    val right = Stream.of[KeyValueRecord]

    val joined = left.fullJoin(right)
    val conditioned = joined.where((l, r) => l.key == r.key)
    val selected = conditioned.select((l, r) => TestFlinkGenJoin.combineRecords(l, r))

    val graph = new StreamGraph(selected)

    val leftInputRecords = List(new KeyValueRecord("1", "l1"), new KeyValueRecord("2", "l2"))
    val rightInputRecords = List(new KeyValueRecord("1", "r1"), new KeyValueRecord("2", "r2"))
    val config = new ApplicationConfiguration
    config.setSource(left, new ListDataSource(leftInputRecords))
    config.setSource(right, new ListDataSource(rightInputRecords))

    val result = TestApplicationExecutor.executeApplication(graph, config, 60, selected)

    val outputs = result.getRecords(selected)
    assertTrue(outputs.contains(new KeyValueRecord("1", "l1r1")))
    assertTrue(outputs.contains(new KeyValueRecord("2", "l2r2")))
  }

  @Test
  def test_FlinkGenJoin_WithFullJoinWithConditionAndSelectToFields_OutputsExpectedJoinedRecords(): Unit = {
    val left = Stream.of[KeyValueRecord]
    val right = Stream.of[KeyValueRecord]

    val joined = left.fullJoin(right)
    val conditioned = joined.where((l, r) => l.key == r.key)
    val selected = conditioned.select((l, r) => fields(
      field("leftValue", if (l == null) "" else l.value),
      field("rightValue", if (r == null) "" else r.value)
    ))

    val graph = new StreamGraph(selected)

    val leftInputRecords = List(new KeyValueRecord("1", "l1"), new KeyValueRecord("2", "l2"))
    val rightInputRecords = List(new KeyValueRecord("1", "r1"), new KeyValueRecord("2", "r2"))
    val config = new ApplicationConfiguration
    config.setSource(left, new ListDataSource(leftInputRecords))
    config.setSource(right, new ListDataSource(rightInputRecords))

    val result = TestApplicationExecutor.executeApplication(graph, config, 60, selected)

    val outputs = result.getRecords(selected)
    assertTrue(outputs.contains(("l1", "r1")))
    assertTrue(outputs.contains(("l2", "r2")))
  }

  @Test
  def test_FlinkGenJoin_WithLeftInnerJoinAndSelectToRecord_WithTwoMatchingRecordsOnRightStream_OutputsOneRecordPerLeftInputRecord(): Unit = {
    val left = Stream.of[KeyValueRecord]
    val right = Stream.of[KeyValueRecord]

    val joined = left.leftInnerJoin(right)
    val conditioned = joined.where((l, r) => l.key == r.key)
    val selected = conditioned.select((l, r) => TestFlinkGenJoin.combineRecords(l, r))

    val graph = new StreamGraph(selected)

    val leftInputRecords = List(KeyValueRecord("1", "l1"), KeyValueRecord("2", "l2"))
    val rightInputRecords = List(KeyValueRecord("1", "r1"), KeyValueRecord("2", "r2"), KeyValueRecord("1", "r1b"), KeyValueRecord("2", "r2b"))
    val config = new ApplicationConfiguration
    config.setSource(left, new ListDataSource(leftInputRecords))
    config.setSource(right, new ListDataSource(rightInputRecords))

    val result = TestApplicationExecutor.executeApplication(graph, config, 60, selected)

    val outputs = result.getRecords(selected)

    // There ought to be two output records, one for each key.
    // We can't guarantee what the joined value will be though.
    assertEquals(2, outputs.length)
    assertTrue(outputs.exists(_.key == "1"))
    assertTrue(outputs.exists(_.key == "2"))
  }
}
