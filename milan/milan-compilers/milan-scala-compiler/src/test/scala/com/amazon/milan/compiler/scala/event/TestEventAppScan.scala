package com.amazon.milan.compiler.scala.event

import com.amazon.milan.compiler.scala.testing.{EventAppTester, IntRecord}
import com.amazon.milan.lang._
import org.junit.Assert._
import org.junit.Test

@Test
class TestEventAppScan {
  @Test
  def test_EventAppScan_WithSummingScanOperation_OutputsExpectedRecords(): Unit = {
    val input = Stream.of[IntRecord].withId("input")
    val output = input.scan(0, (s: Int, r: IntRecord) => (s + r.i, IntRecord(s + r.i))).withId("output")

    val outputRecords = EventAppTester.execute(output, input, IntRecord(1), IntRecord(2), IntRecord(2), IntRecord(4))

    assertEquals(List(IntRecord(1), IntRecord(3), IntRecord(5), IntRecord(9)), outputRecords)
  }
}
