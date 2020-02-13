package com.amazon.milan.lang

import com.amazon.milan.program.{ExternalStream, FunctionDef, LatestBy, Ref, SelectField, SelectTerm}
import com.amazon.milan.test.{DateKeyValueRecord, IntStringRecord, Tuple3Record}
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

  @Test
  def test_Stream_LatestBy_ReturnsWindowedStreamWithExpectedExpressions(): Unit = {
    val input = Stream.of[DateKeyValueRecord].withId("input")
    val windowed = input.latestBy(r => r.dateTime, r => r.key)

    val LatestBy(ExternalStream("input", "input", _), dateFunc, keyFunc) = windowed.expr
    val FunctionDef(List("r"), SelectField(SelectTerm("r"), "dateTime")) = dateFunc
    val FunctionDef(List("r"), SelectField(SelectTerm("r"), "key")) = keyFunc
  }
}
