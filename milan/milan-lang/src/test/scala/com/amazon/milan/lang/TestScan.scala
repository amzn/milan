package com.amazon.milan.lang

import com.amazon.milan.program.{ConstantValue, FunctionDef, Scan, Tuple}
import com.amazon.milan.test.IntRecord
import com.amazon.milan.typeutil.TypeDescriptor
import org.junit.Assert._
import org.junit.Test


@Test
class TestScan {
  @Test
  def test_Scan_ProducesExpectedExpressionTree(): Unit = {
    val input = Stream.of[IntRecord].withId("input")
    val output = input.scan(0, (s: Int, r: IntRecord) => (s + r.i, IntRecord(s + r.i))).withId("output")

    val Scan(_, ConstantValue(0, _), FunctionDef(_, Tuple(_))) = output.expr
    assertEquals(TypeDescriptor.of[IntRecord], output.recordType)
  }
}
