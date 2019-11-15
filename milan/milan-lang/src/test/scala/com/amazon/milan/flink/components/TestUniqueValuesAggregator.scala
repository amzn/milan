package com.amazon.milan.flink.components

import com.amazon.milan.flink.compiler.internal.RuntimeCompiledFunction
import com.amazon.milan.test.IntKeyValueRecord
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.junit.Assert._
import org.junit.Test


@Test
class TestUniqueValuesAggregator {
  @Test
  def test_UniqueValuesAggregator_WithCollisionResolverAlwaysReturnsNewValue_OutputsLastInputValueAsResult(): Unit = {
    val keyExtractor = RuntimeCompiledFunction.create((r: IntKeyValueRecord) => r.key)
    val collisionResolver = RuntimeCompiledFunction.create((currentValue: IntKeyValueRecord, newValue: IntKeyValueRecord) => newValue)
    val outputFunction = RuntimeCompiledFunction.create((r: Iterable[IntKeyValueRecord]) => r.head)

    val inputTypeInfo = createTypeInformation[IntKeyValueRecord]
    val keyTypeInfo = createTypeInformation[Int]
    val outputTypeInfo = createTypeInformation[IntKeyValueRecord]

    val target = new UniqueValuesAggregator[IntKeyValueRecord, Int, IntKeyValueRecord](
      keyExtractor,
      collisionResolver,
      outputFunction,
      inputTypeInfo,
      keyTypeInfo,
      outputTypeInfo)

    val values = List(
      IntKeyValueRecord(1, 1),
      IntKeyValueRecord(1, 3),
      IntKeyValueRecord(1, 2)
    )

    val result = add(target, values: _*)
    assertEquals(IntKeyValueRecord(1, 2), result)
  }

  @Test
  def test_UniqueValuesAggregator_WithCollisionResolverReturnsRecordWithLargestValue_OutputsInputRecordWithLargestValue(): Unit = {
    val keyExtractor = RuntimeCompiledFunction.create((r: IntKeyValueRecord) => r.key)
    val collisionResolver = RuntimeCompiledFunction.create((currentValue: IntKeyValueRecord, newValue: IntKeyValueRecord) => if (currentValue.value > newValue.value) currentValue else newValue)
    val outputFunction = RuntimeCompiledFunction.create((r: Iterable[IntKeyValueRecord]) => r.head)

    val inputTypeInfo = createTypeInformation[IntKeyValueRecord]
    val keyTypeInfo = createTypeInformation[Int]
    val outputTypeInfo = createTypeInformation[IntKeyValueRecord]

    val target = new UniqueValuesAggregator[IntKeyValueRecord, Int, IntKeyValueRecord](
      keyExtractor,
      collisionResolver,
      outputFunction,
      inputTypeInfo,
      keyTypeInfo,
      outputTypeInfo)

    val values = List(
      IntKeyValueRecord(1, 1),
      IntKeyValueRecord(1, 3),
      IntKeyValueRecord(1, 2)
    )

    val result = add(target, values: _*)
    assertEquals(IntKeyValueRecord(1, 3), result)
  }

  private def add[TIn, TAcc, TOut](agg: AggregateFunction[TIn, TAcc, TOut], values: TIn*): TOut = {
    val finalAccumulator = values.foldLeft(agg.createAccumulator())((acc, value) => agg.add(value, acc))
    agg.getResult(finalAccumulator)
  }
}
