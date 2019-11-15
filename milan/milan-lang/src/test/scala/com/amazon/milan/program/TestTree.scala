package com.amazon.milan.program

import com.amazon.milan.typeutil.types
import org.junit.Assert._
import org.junit.Test


@Test
class TestTree {
  @Test
  def test_Tree_ToString_ForApplyFunctionTree_ReturnsExpectedString(): Unit = {
    val tree = new ApplyFunction(FunctionReference("MyType", "MyFunction"), List(new SelectField(SelectTerm("foo"), "bar")), types.Boolean)
    val str = tree.toString
    assertEquals("ApplyFunction(FunctionReference(\"MyType\", \"MyFunction\"), List(SelectField(SelectTerm(\"foo\"), \"bar\")), TypeDescriptor(\"Boolean\"))", str)
  }

  @Test
  def test_Tree_ToString_ForConstantValueOfInteger_ReturnsExpectedString(): Unit = {
    val tree = new ConstantValue(1, types.Int)
    val str = tree.toString
    assertEquals("ConstantValue(1, TypeDescriptor(\"Int\"))", str)
  }
}
