package com.amazon.milan.compiler.scala.event

import com.amazon.milan.compiler.scala.testing.{EventAppTester, IntRecord}
import com.amazon.milan.lang._
import org.junit.Assert._
import org.junit.Test

@Test
class TestEventAppFilter {
  @Test
  def test_EventAppFilter_FiltersRecords(): Unit = {
    val input = Stream.of[IntRecord].withId("input")
    val output = input.where(r => r.i > 5).withId("output")

    val results = EventAppTester.execute(output, input, IntRecord(1), IntRecord(6), IntRecord(3), IntRecord(10))
    assertEquals(List(IntRecord(6), IntRecord(10)), results)
  }
}
