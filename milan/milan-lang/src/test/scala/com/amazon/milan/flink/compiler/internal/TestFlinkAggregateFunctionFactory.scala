package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.lang.aggregation.{mean, min, sum}
import com.amazon.milan.program.testing._
import com.amazon.milan.program.{FunctionDef, SelectTerm, Tree, TypeChecker}
import com.amazon.milan.test.{IntKeyValueRecord, IntRecord, NumbersRecord}
import com.amazon.milan.typeutil.{TypeDescriptor, types}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.{Tuple1, Tuple2, Tuple3}
import org.junit.Assert._
import org.junit.Test


object TestFlinkAggregateFunctionFactory {
  val typeName: String = getClass.getTypeName.stripSuffix("$")

  def toDouble(i: Int): Double = i.toDouble

  val identityFunction: FunctionDef = new FunctionDef(List("x"), new SelectTerm("x"))
}

import com.amazon.milan.flink.compiler.internal.TestFlinkAggregateFunctionFactory._


@Test
class TestFlinkAggregateFunctionFactory {
  @Test
  def test_FlinkAggregateFunctionFactory_CreateIntermediateAggregateFunction_WithSumPlusMinOfInputInt_ThenAdd_ReturnsExpectedResult(): Unit = {
    val mapFunctionDef = Tree.fromFunction((key: Int, i: IntRecord) => sum(i.i) + min(i.i))
    TypeChecker.typeCheck(mapFunctionDef, List(types.Int, TypeDescriptor.of[IntRecord]))

    val groupExpr = createGroupBy[IntRecord](identityFunction)

    val agg =
      FlinkAggregateFunctionFactory.createIntermediateAggregateFunction(mapFunctionDef, "com.amazon.milan.test.IntRecord", groupExpr)
        .asInstanceOf[AggregateFunction[IntRecord, Tuple2[Int, Option[Int]], Tuple2[Int, Int]]]

    val result = add(agg, IntRecord(1), IntRecord(2), IntRecord(3))
    val expectedResult = new Tuple2[Int, Int](1 + 2 + 3, 1)
    assertEquals(expectedResult, result)
  }

  @Test
  def test_FlinkAggregateFunctionFactory_CreateIntermediateAggregateFunction_WithComplicatedFunction_ThenAdd_ReturnsExpectedResult(): Unit = {
    val mapFunctionDef = Tree.fromFunction((key: Int, r: NumbersRecord) => sum(r.i1 - r.i2) + min(r.d1 + toDouble(r.i2) - r.d2) - mean(r.f1))
    TypeChecker.typeCheck(mapFunctionDef, List(types.Int, TypeDescriptor.of[NumbersRecord]))

    val groupExpr = createGroupBy[NumbersRecord](Tree.fromFunction((r: NumbersRecord) => r.i1))

    val agg =
      FlinkAggregateFunctionFactory.createIntermediateAggregateFunction(
        mapFunctionDef,
        "com.amazon.milan.test.NumbersRecord",
        groupExpr).asInstanceOf[AggregateFunction[NumbersRecord, Tuple3[Int, Option[Double], (Long, Float)], Tuple3[Int, Double, Double]]]

    val result = add(agg, NumbersRecord(1, 2, 3, 4, 5), NumbersRecord(6, 7, 8, 9, 10), NumbersRecord(1, 3, 5, 7, 9))
    val expectedResult = new Tuple3[Int, Double, Double]((1 - 2) + (6 - 7) + (1 - 3), Math.min(3 + 2 - 4, Math.min(8 + 7 - 9, 5 + 3 - 7)), (5.0 + 10.0 + 9.0) / 3.0)
    assertEquals(expectedResult, result)
  }

  @Test
  def test_FlinkAggregateFunctionFactory_CreateIntermediateAggregateFunction_WithSumOfInputPlusKey_ThenAdd_ReturnsSumOfInputsAndIgnoresKey(): Unit = {
    val mapFunctionDef = Tree.fromFunction((key: Int, r: IntKeyValueRecord) => key + sum(r.value))
    TypeChecker.typeCheck(mapFunctionDef, List(types.Int, TypeDescriptor.of[IntKeyValueRecord]))

    val groupExpr = createGroupBy[IntKeyValueRecord](Tree.fromFunction((r: IntKeyValueRecord) => r.key))

    val agg =
      FlinkAggregateFunctionFactory.createIntermediateAggregateFunction(
        mapFunctionDef,
        "com.amazon.milan.test.IntKeyValueRecord",
        groupExpr).asInstanceOf[AggregateFunction[IntKeyValueRecord, Tuple1[Int], Tuple1[Int]]]

    val result = add(agg, IntKeyValueRecord(1, 3), IntKeyValueRecord(2, 3), IntKeyValueRecord(3, 5))
    assertEquals(3 + 3 + 5, result.f0)
  }

  def add[TIn, TAcc, TOut](agg: AggregateFunction[TIn, TAcc, TOut], values: TIn*): TOut = {
    val finalAccumulator = values.foldLeft(agg.createAccumulator())((acc, value) => agg.add(value, acc))
    agg.getResult(finalAccumulator)
  }
}
