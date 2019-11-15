package com.amazon.milan.program

import com.amazon.milan.test.{IntRecord, KeyValueRecord}
import com.amazon.milan.typeutil.{TypeDescriptor, types}
import org.junit.Test


@Test
class TestConvertExpression {
  @Test
  def test_ConvertExpression_NewObject_ReturnsExpectedExpressionTree(): Unit = {
    val tree = Tree.fromExpression(new IntRecord(0))
    val CreateInstance(TypeDescriptor("com.amazon.milan.test.IntRecord"), List(ConstantValue(0, types.Int))) = tree
  }

  @Test
  def test_ConvertExpression_TupleFunction_ReturnsExpectedExpressionTree(): Unit = {
    val tree = Tree.fromExpression((a: Int, b: (Int, Int)) => b match {
      case (x, y) => a == x
    })

    val FunctionDef(List("a", "b"), Unpack(SelectTerm("b"), List("x", "y"), Equals(SelectTerm("a"), SelectTerm("x")))) = tree
  }

  @Test
  def test_ConvertExpression_TwoArgumentFunction_ThatReferencesOneArgument_ReturnsSelectTerm(): Unit = {
    val tree = Tree.fromExpression((a: Int, b: String) => a)
    val FunctionDef(List("a", "b"), SelectTerm("a")) = tree
  }

  @Test
  def test_ConvertExpression_OneArgumentFunction_ThatReferencesAFieldOfThatArgument_ReturnsSelectField(): Unit = {
    val tree = Tree.fromExpression((a: IntRecord) => a.i)
    val FunctionDef(List("a"), SelectField(SelectTerm("a"), "i")) = tree
  }

  @Test
  def test_ConvertExpression_TwoArgumentFunction_WithOneRecordAndOneTupleArgument_ThatUnpacksTupleArgumentAndUsesAllFields_ReturnsExpectedTree(): Unit = {
    val tree = Tree.fromExpression((c: KeyValueRecord, ab: (KeyValueRecord, KeyValueRecord)) => ab match {
      case (a, b) => a != null && b != null && c != null
    })
    val FunctionDef(List("c", "ab"), Unpack(SelectTerm("ab"), List("a", "b"), And(And(Not(IsNull(SelectTerm("a"))), Not(IsNull(SelectTerm("b")))), Not(IsNull(SelectTerm("c")))))) = tree
  }

  @Test
  def test_ConvertExpression_WithSimpleExpressionThatUsesLocalIntVariable_ConvertsVariableValueIntoConstant(): Unit = {
    val threshold = 5
    val tree = Tree.fromExpression((i: Int) => i > threshold)
    val FunctionDef(List("i"), GreaterThan(SelectTerm("i"), ConstantValue(5, types.Int))) = tree
  }

  @Test
  def test_ConvertExpression_WithSimpleExpressionThatUsesLocalStringVariable_ConvertsVariableValueIntoConstant(): Unit = {
    val value = "value"
    val tree = Tree.fromExpression((s: String) => s == value)
    val FunctionDef(List("s"), Equals(SelectTerm("s"), ConstantValue(s, types.String))) = tree
  }

  @Test
  def test_ConvertExpression_WithMatchExpressionThatUsesLocalIntVariable_ConvertsVariableValueIntoConstant(): Unit = {
    val threshold = 5
    val tree = Tree.fromExpression((t: (Int, Long)) => t match {
      case (i, _) => i > threshold
    })
    val FunctionDef(List("t"), Unpack(SelectTerm("t"), List("i", "_"), GreaterThan(SelectTerm("i"), ConstantValue(5, types.Int)))) = tree
  }

  @Test
  def test_ConvertExpression_WithSimpleExpressionThatUsesIntArgument_ConvertsVariableValueIntoConstant(): Unit = {
    def getTree(threshold: Int): Tree =
      Tree.fromExpression((i: Int) => i > threshold)

    val tree = getTree(5)
    val FunctionDef(List("i"), GreaterThan(SelectTerm("i"), ConstantValue(5, types.Int))) = tree
  }
}
