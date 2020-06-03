package com.amazon.milan.program

import com.amazon.milan.typeutil.TypeDescriptor
import org.junit.Test


@Test
class TestTreeParser {
  @Test
  def test_TreeParser_Parse_WithFunctionTree_ReturnsMatchingTreeStructure(): Unit = {
    val tree = TreeParser.parse[FunctionDef]("FunctionDef(List(ValueDef(\"x\"), ValueDef(\"y\")), Equals(SelectTerm(\"x\"), SelectTerm(\"y\")))")
    val FunctionDef(List(ValueDef("x", _), ValueDef("y", _)), Equals(SelectTerm("x"), SelectTerm("y"))) = tree
  }

  @Test
  def test_TreeParser_Parse_WithConstantIntValueTree_ReturnsMatchingTreeStructure(): Unit = {
    val tree = TreeParser.parse[ConstantValue]("ConstantValue(1, TypeDescriptor(\"Int\"))")
    val ConstantValue(1, TypeDescriptor("Int")) = tree
  }
}
