package com.amazon.milan.compiler.scala.event

import com.amazon.milan.compiler.scala.testing.{EventAppTester, IntRecord}
import com.amazon.milan.lang
import org.junit.Assert._
import org.junit.Test


@Test
class TestEventAppMap {
  @Test
  def test_EventAppMap_OfDataStream_MapsRecords(): Unit = {
    val input = lang.Stream.of[IntRecord].withId("input")
    val output = input.map(r => IntRecord(r.i + 1)).withId("output")

    val outputRecords = EventAppTester.execute(output, input, IntRecord(1), IntRecord(2))

    assertEquals(List(IntRecord(2), IntRecord(3)), outputRecords)
  }
}
