package com.amazon.milan.flink.components

import com.amazon.milan.flink.api.MilanAggregateFunction
import com.amazon.milan.flink.compiler.internal.{RuntimeCompiledFunction, _}
import com.amazon.milan.program.FunctionDef
import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation


object InputMappingAggregateFunctionWrapper {
  val typeName: String = getClass.getTypeName.stripSuffix("$")
}


class InputMappingAggregateFunctionWrapper[TIn, TMap, TAcc, TOut](mapFunction: SerializableFunction[TIn, TMap],
                                                                  innerAggregateFunction: MilanAggregateFunction[TMap, TAcc, TOut])
  extends MilanAggregateFunction[TIn, TAcc, TOut] {

  def this(inputType: TypeDescriptor[_],
           mapFunctionDef: FunctionDef,
           innerAggregateFunction: MilanAggregateFunction[TMap, TAcc, TOut]) {
    this(new RuntimeCompiledFunction[TIn, TMap](inputType, mapFunctionDef), innerAggregateFunction)
  }

  override def add(in: TIn, acc: TAcc): TAcc = {
    val extracted = this.mapFunction(in)
    this.innerAggregateFunction.add(extracted, acc)
  }

  override def createAccumulator(): TAcc =
    this.innerAggregateFunction.createAccumulator()

  override def getResult(acc: TAcc): TOut =
    this.innerAggregateFunction.getResult(acc)

  override def merge(acc: TAcc, acc1: TAcc): TAcc =
    this.innerAggregateFunction.merge(acc, acc1)

  override def getProducedType: TypeInformation[TOut] = innerAggregateFunction.getProducedType

  override def getAccumulatorType: TypeInformation[TAcc] = innerAggregateFunction.getAccumulatorType
}
