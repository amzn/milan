package com.amazon.milan.compiler.flink.runtime

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable


abstract class InputMappingAggregateFunctionWrapper[TIn, TMap, TAcc, TOut](innerAggregateFunction: AggregateFunction[TMap, TAcc, TOut] with ResultTypeQueryable[TOut])
  extends AggregateFunction[TIn, TAcc, TOut]
    with ResultTypeQueryable[TOut] {

  protected def mapInput(input: TIn): TMap

  override def add(in: TIn, acc: TAcc): TAcc = {
    val extracted = this.mapInput(in)
    this.innerAggregateFunction.add(extracted, acc)
  }

  override def createAccumulator(): TAcc =
    this.innerAggregateFunction.createAccumulator()

  override def getResult(acc: TAcc): TOut =
    this.innerAggregateFunction.getResult(acc)

  override def merge(acc: TAcc, acc1: TAcc): TAcc =
    this.innerAggregateFunction.merge(acc, acc1)

  override def getProducedType: TypeInformation[TOut] = innerAggregateFunction.getProducedType
}
