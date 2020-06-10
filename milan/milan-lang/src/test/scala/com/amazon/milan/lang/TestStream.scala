package com.amazon.milan.lang

import com.amazon.milan.program.ExternalStream
import com.amazon.milan.test.{IntStringRecord, Tuple3Record}
import org.junit.Assert._
import org.junit.Test


@Test
class TestStream {
  @Test
  def test_Stream_Of_ReturnsStreamWithExternalStreamNodeAndCorrectTypeName(): Unit = {
    val input = Stream.of[IntStringRecord]

    val ExternalStream(_, _, streamType) = input.expr
    assertEquals(TypeUtil.getTypeName(classOf[IntStringRecord]), streamType.recordType.fullName)
  }

  @Test
  def test_Stream_Of_GenericType_HasRecordTypeWithGenericArguments(): Unit = {
    val input = Stream.of[Tuple3Record[Int, Long, Double]]

    val ExternalStream(_, _, streamType) = input.expr
    assertEquals(3, streamType.recordType.genericArguments.length)
  }
}
