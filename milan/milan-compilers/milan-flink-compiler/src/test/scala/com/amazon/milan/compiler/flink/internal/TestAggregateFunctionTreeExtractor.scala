package com.amazon.milan.compiler.flink.internal

import com.amazon.milan.compiler.flink.testing.TwoIntRecord
import com.amazon.milan.compiler.scala.trees.AggregateFunctionTreeExtractor
import com.amazon.milan.lang.aggregation._
import com.amazon.milan.program._
import com.amazon.milan.typeutil.{createTypeDescriptor, types}
import org.junit.Assert._
import org.junit.Test


@Test
class TestAggregateFunctionTreeExtractor {
  @Test
  def test_AggregateFunctionTreeExtractor_GetAggregateInputFunctions_WithAggregateFunctionMeanMinusMin_ReturnsTwoFunctionsOfTheAggregateFunctionArguments(): Unit = {
    val function = Tree.fromExpression((key: Int, r: TwoIntRecord) => mean(r.a) - min(r.b)).asInstanceOf[FunctionDef]
    TypeChecker.typeCheck(function, List(types.Int, createTypeDescriptor[TwoIntRecord]))

    val functions = AggregateFunctionTreeExtractor.getAggregateInputFunctionsWithKey(function)
    assertEquals(2, functions.length)

    // These will throw an exception if the pattern doesn't match.
    val FunctionDef(List(ValueDef("r", _)), SelectField(SelectTerm("r"), "a")) = functions.head
    val FunctionDef(List(ValueDef("r", _)), SelectField(SelectTerm("r"), "b")) = functions.last
  }

  @Test
  def test_AggregateFunctionTreeExtractor_GetResultTupleToOutputFunction_WithAggregateFunctionMeanMinusMin_ReturnsFunctionOfResultTupleThatSubtractsTheTupleElements(): Unit = {
    val function = Tree.fromExpression((key: Int, r: TwoIntRecord) => mean(r.a) - min(r.b)).asInstanceOf[FunctionDef]
    TypeChecker.typeCheck(function, List(types.Int, createTypeDescriptor[TwoIntRecord]))

    val output = AggregateFunctionTreeExtractor.getResultTupleToOutputFunctionWithKey(function)
    val FunctionDef(List(ValueDef("key", _), ValueDef("result", _)), Minus(TupleElement(SelectTerm("result"), 0), TupleElement(SelectTerm("result"), 1))) = output
  }

  @Test
  def test_AggregateFunctionTreeExtractor_GetAggregateFunctionReferences_WithAggregateFunctionMeanMinusMin_ReturnsFunctionReferencesForMeanAndMin(): Unit = {
    val function = Tree.fromExpression((key: Int, r: TwoIntRecord) => mean(r.a) - min(r.b)).asInstanceOf[FunctionDef]
    val exprs = AggregateFunctionTreeExtractor.getAggregateExpressions(function)
    assertTrue(exprs.exists(_.isInstanceOf[Mean]))
    assertTrue(exprs.exists(_.isInstanceOf[Min]))
  }

  @Test
  def test_AggregateFunctionTreeExtractor_GetResultTupleToOutputFunction_WithFunctionContainingSumPlusKey_ReturnsExpectedFunctionOfKeyAndResult(): Unit = {
    val function = Tree.fromExpression((key: Int, r: TwoIntRecord) => sum(r.a) + key).asInstanceOf[FunctionDef]
    TypeChecker.typeCheck(function, List(types.Int, createTypeDescriptor[TwoIntRecord]))

    val mapFunc = AggregateFunctionTreeExtractor.getResultTupleToOutputFunctionWithKey(function)
    val FunctionDef(List(ValueDef("key", _), ValueDef("result", _)), Plus(TupleElement(SelectTerm("result"), 0), SelectTerm("key"))) = mapFunc
  }

  @Test(expected = classOf[InvalidProgramException])
  def test_AggregateFunctionTreeExtractor_GetAggregateInputFunctions_WithKeyUsedInsideAggregateFunctionArgument_ThrowsInvalidProgramException(): Unit = {
    val function = Tree.fromExpression((key: Int, r: Int) => sum(key)).asInstanceOf[FunctionDef]
    TypeChecker.typeCheck(function, List(types.Int, types.Int))

    AggregateFunctionTreeExtractor.getAggregateInputFunctionsWithKey(function)
  }
}
