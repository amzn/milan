package com.amazon.milan.compiler.scala.trees

import com.amazon.milan.compiler.scala.testing.TwoIntRecord
import com.amazon.milan.program.{ConstantValue, FunctionDef, Minus, Plus, SelectField, SelectTerm, Tree, Tuple, TypeChecker, ValueDef}
import org.junit.Test


@Test
class TestKeySelectorExtractor {
  @Test
  def test_KeySelectorExtractor_GetKeyTupleFunctions_WithTwoEqualityConditions_ReturnsFunctionsThatProduceExpectedTuples(): Unit = {
    val func = Tree.fromFunction((x: TwoIntRecord, y: TwoIntRecord) => x.a == y.a && x.b + 1 == y.b - 1)
    TypeChecker.typeCheck(func)
    val (leftFunc, rightFunc) = KeySelectorExtractor.getKeyTupleFunctions(func)

    val FunctionDef(List(ValueDef("x", _)), Tuple(List(SelectField(SelectTerm("x"), "a"), Plus(SelectField(SelectTerm("x"), "b"), ConstantValue(1, _))))) = leftFunc
    val FunctionDef(List(ValueDef("y", _)), Tuple(List(SelectField(SelectTerm("y"), "a"), Minus(SelectField(SelectTerm("y"), "b"), ConstantValue(1, _))))) = rightFunc
  }
}
