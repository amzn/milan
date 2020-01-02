package com.amazon.milan.flink.components

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.scala._
import org.junit.Assert._
import org.junit.Test


@Test
class TestMultiAggregateFunction {
  @Test
  def test_MultiAggregateFunction_CombineAggregateFunctions_WithSumAndMax_ThenAddAndMergeAndGetResult_ReturnsTupleWithExpectedValues(): Unit = {
    val functions = List(
      new BuiltinAggregateFunctions.Sum[Int](createTypeInformation[Int]),
      new BuiltinAggregateFunctions.Max[Int](createTypeInformation[Int])
    )

    val target = MultiAggregateFunction.combineAggregateFunctions[Int](functions, "Int").asInstanceOf[AggregateFunction[Int, Tuple2[Option[Int], Option[Int]], Tuple2[Int, Int]]]
    assertEquals(new Tuple2(Some(4), Some(3)), target.add(3, new Tuple2(Some(1), Some(2))))
    assertEquals(new Tuple2(Some(5), Some(7)), target.merge(new Tuple2(Some(4), Some(3)), new Tuple2(Some(1), Some(7))))
    assertEquals(new Tuple2(7, 8), target.getResult(new Tuple2(Some(7), Some(8))))
  }

  @Test
  def test_MultiAggregateFunction_CombineAggregateFunctions_WithSumAndMean_ThenAddAndGetResult_ReturnsTupleWithExpectedValues(): Unit = {
    val functions = List(
      new BuiltinAggregateFunctions.Sum[Double](createTypeInformation[Double]),
      new BuiltinAggregateFunctions.Mean[Double](createTypeInformation[Double])
    )

    val target = MultiAggregateFunction.combineAggregateFunctions[Double](functions, "Int").asInstanceOf[AggregateFunction[Double, Tuple2[Option[Double], Tuple2[Long, Double]], Tuple2[Double, Double]]]
    val acc = target.createAccumulator()
    val acc1 = target.add(3.0, acc)
    val acc2 = target.add(4.0, acc1)
    val result = target.getResult(acc2)
    assertEquals(7.0, result.f0, 1e-10)
    assertEquals(3.5, result.f1, 1e-10)
  }

  @Test
  def test_MultiAggregateFunction_CombineAggregateFunctions_WithArgMinAndArgMax_ThenAddAndGetResult_ReturnsTupleWithExpectedValues(): Unit = {
    val functions = List(
      new BuiltinAggregateFunctions.ArgMin[Int, Int](createTypeInformation[Int], createTypeInformation[Int]),
      new BuiltinAggregateFunctions.ArgMax[Int, Int](createTypeInformation[Int], createTypeInformation[Int])
    )

    val target = MultiAggregateFunction.combineAggregateFunctions[Tuple2[Int, Int]](functions, "Int")
      .asInstanceOf[AggregateFunction[Tuple2[Int, Int], Tuple2[Tuple2[Option[Int], Option[Int]], Tuple2[Option[Int], Option[Int]]], Tuple2[Int, Int]]]

    val acc = target.createAccumulator()
    val acc1 = target.add(new Tuple2(2, 0), acc)
    val acc2 = target.add(new Tuple2(1, 5), acc1)
    val acc3 = target.add(new Tuple2(3, 10), acc2)
    val result = target.getResult(acc3)
    assertEquals(new Tuple2(5, 10), result)
  }
}
