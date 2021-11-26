package com.amazon.milan.compiler.scala.event

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.compiler.scala.testing.KeyValueRecord
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang._
import com.amazon.milan.lang.aggregation._
import org.junit.Assert._
import org.junit.Test


@Test
class TestEventAppSelect {
  @Test
  def test_EventAppSelectSum_OutputsRunningSumPerGroup(): Unit = {
    val input = Stream.of[KeyValueRecord].withId("input")
    val grouped = input.groupBy(r => r.key).withId("grouped")
    val output = grouped.select((key, r) => KeyValueRecord(key, sum(r.value)))

    val streams = StreamCollection.build(output)

    val config = new ApplicationConfiguration()
    val sink = config.addMemorySink(output)

    val target = EventAppTester.compile(streams, config)

    target.consume("input", KeyValueRecord(1, 1))
    assertEquals(KeyValueRecord(1, 1), sink.getValues.last)

    target.consume("input", KeyValueRecord(1, 2))
    assertEquals(KeyValueRecord(1, 3), sink.getValues.last)

    target.consume("input", KeyValueRecord(2, 3))
    assertEquals(KeyValueRecord(2, 3), sink.getValues.last)

    target.consume("input", KeyValueRecord(2, 5))
    assertEquals(KeyValueRecord(2, 8), sink.getValues.last)
  }
}