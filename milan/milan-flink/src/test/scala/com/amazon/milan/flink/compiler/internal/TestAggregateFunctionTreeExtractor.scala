package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.flink.testutil._
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

    val functions = AggregateFunctionTreeExtractor.getAggregateInputFunctions(function)
    assertEquals(2, functions.length)

    // These will throw an exception if the pattern doesn't match.
    val FunctionDef(List("r"), SelectField(SelectTerm("r"), "a")) = functions.head
    val FunctionDef(List("r"), SelectField(SelectTerm("r"), "b")) = functions.last
  }

  @Test
  def test_AggregateFunctionTreeExtractor_GetResultTupleToOutputFunction_WithAggregateFunctionMeanMinusMin_ReturnsFunctionOfResultTupleThatSubtractsTheTupleElements(): Unit = {
    val function = Tree.fromExpression((key: Int, r: TwoIntRecord) => mean(r.a) - min(r.b)).asInstanceOf[FunctionDef]
    TypeChecker.typeCheck(function, List(types.Int, createTypeDescriptor[TwoIntRecord]))

    val output = AggregateFunctionTreeExtractor.getResultTupleToOutputFunction(function)
    val FunctionDef(List("key", "result"), Minus(SelectField(SelectTerm("result"), "f0"), SelectField(SelectTerm("result"), "f1"))) = output
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

    val mapFunc = AggregateFunctionTreeExtractor.getResultTupleToOutputFunction(function)
    val FunctionDef(List("key", "result"), Plus(SelectField(SelectTerm("result"), "f0"), SelectTerm("key"))) = mapFunc
  }

  @Test(expected = classOf[InvalidProgramException])
  def test_AggregateFunctionTreeExtractor_GetAggregateInputFunctions_WithKeyUsedInsideAggregateFunctionArgument_ThrowsInvalidProjectException(): Unit = {
    val function = Tree.fromExpression((key: Int, r: Int) => sum(key)).asInstanceOf[FunctionDef]
    TypeChecker.typeCheck(function, List(types.Int, types.Int))

    AggregateFunctionTreeExtractor.getAggregateInputFunctions(function)
  }
}
