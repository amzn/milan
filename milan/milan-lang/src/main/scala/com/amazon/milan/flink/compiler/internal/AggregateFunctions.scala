package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.program.AggregateExpression
import org.apache.flink.api.java.tuple.Tuple2


object AggregateFunctions {
  val typeName: String = getClass.getTypeName.stripSuffix("$")

  def getAggregateExpressionFunctionFullName(expr: AggregateExpression): String = {
    val methodName = expr.expressionType.substring(0, 1).toLowerCase + expr.expressionType.substring(1)
    s"${this.typeName}.$methodName"
  }

  def sum[T: Numeric](values: Iterable[T]): T = values.sum

  def min[T: Ordering](values: Iterable[T]): T = values.min

  def max[T: Ordering](values: Iterable[T]): T = values.max

  def mean[T: Numeric](values: Iterable[T]): Double = {
    val numeric = implicitly[Numeric[T]]
    var total = numeric.zero
    var count = 0

    for (value <- values) {
      total = numeric.plus(total, value)
      count += 1
    }

    numeric.toDouble(total) / count.toDouble
  }

  def argMin[TArg: Ordering, TValue](values: Iterable[Tuple2[TArg, TValue]]): TValue = values.minBy(_.f0).f1

  def argMax[TArg: Ordering, TValue](values: Iterable[Tuple2[TArg, TValue]]): TValue = values.maxBy(_.f0).f1

  def first[T](values: Iterable[T]): T = values.head
}
