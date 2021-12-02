package com.amazon.milan.compiler.scala.event

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.compiler.scala.testing.{EventAppTester, KeyValueRecord, TwoIntRecord}
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang._
import org.junit.Assert._
import org.junit.Test


object TestEventAppJoin {
  def joinRecords(left: TwoIntRecord, right: TwoIntRecord): TwoIntRecord = {
    if (left == null) {
      TwoIntRecord(right.a, right.b)
    }
    else if (right == null) {
      TwoIntRecord(left.a, left.b)
    }
    else {
      TwoIntRecord(left.a, left.b + right.b)
    }
  }
}


@Test
class TestEventAppJoin {
  @Test
  def test_EventAppJoin_WithLeftJoin(): Unit = {
    val leftInput = Stream.of[KeyValueRecord].withId("left")
    val rightInput = Stream.of[KeyValueRecord].withId("right")

    val joined = leftInput.leftJoin(rightInput).where((l, r) => l.key == r.key && r != null)
    val output = joined.select((l, r) => KeyValueRecord(l.key, l.value + r.value))

    val streams = StreamCollection.build(output)
    val config = new ApplicationConfiguration()
    val sink = config.addMemorySink(output)

    val target = EventAppTester.compile(streams, config)

    // Add a left record, nothing should come out because of the requirement that r != null.
    target.consume("left", KeyValueRecord(1, 1))
    assertEquals(0, sink.getRecordCount)

    // Add a right record, nothing should come out because LeftEnrichmentJoin doesn't output anything for right records.
    target.consume("right", KeyValueRecord(1, 2))
    assertEquals(0, sink.getRecordCount)

    // Add a left record, we should get an output now.
    target.consume("left", KeyValueRecord(1, 3))
    assertEquals(1, sink.getRecordCount)
    assertEquals(KeyValueRecord(1, 5), sink.getValues.head)

    // Using a different key, Add a two right records and then a left record.
    target.consume("right", KeyValueRecord(2, 1))
    target.consume("right", KeyValueRecord(2, 2))
    target.consume("left", KeyValueRecord(2, 4))
    assertEquals(2, sink.getRecordCount)
    assertEquals(KeyValueRecord(2, 6), sink.getValues.last)
  }

  @Test
  def test_EventAppJoin_WithFullJoin_OutputsRecordForEachInput(): Unit = {
    val leftInput = Stream.of[TwoIntRecord].withId("left")
    val rightInput = Stream.of[TwoIntRecord].withId("right")
    val joined = leftInput.fullJoin(rightInput).where((l, r) => l.a == r.a)
    val output = joined.select((l, r) => TestEventAppJoin.joinRecords(l, r)).withId("output")

    val streams = StreamCollection.build(output)
    val config = new ApplicationConfiguration()
    val sink = config.addMemorySink(output)

    val target = EventAppTester.compile(streams, config)

    // Send some records and check the last output after each.
    target.consume("left", TwoIntRecord(1, 1))
    assertEquals(TwoIntRecord(1, 1), sink.getValues.last)

    target.consume("left", TwoIntRecord(1, 3))
    assertEquals(TwoIntRecord(1, 3), sink.getValues.last)

    target.consume("right", TwoIntRecord(1, 2))
    assertEquals(TwoIntRecord(1, 5), sink.getValues.last)

    target.consume("left", TwoIntRecord(1, 5))
    assertEquals(TwoIntRecord(1, 7), sink.getValues.last)

    // Do the same thing as above but from the opposite streams.
    target.consume("right", TwoIntRecord(2, 1))
    assertEquals(TwoIntRecord(2, 1), sink.getValues.last)

    target.consume("right", TwoIntRecord(2, 3))
    assertEquals(TwoIntRecord(2, 3), sink.getValues.last)

    target.consume("left", TwoIntRecord(2, 2))
    assertEquals(TwoIntRecord(2, 5), sink.getValues.last)

    target.consume("right", TwoIntRecord(2, 5))
    assertEquals(TwoIntRecord(2, 7), sink.getValues.last)
  }
}
