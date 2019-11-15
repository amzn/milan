package com.amazon.milan.flink.api

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.typeutils.ResultTypeQueryable


trait MilanAggregateFunction[TIn, TAcc, TOut]
  extends AggregateFunction[TIn, TAcc, TOut]
    with ResultTypeQueryable[TOut]
    with AccumulatorTypeQueryable[TAcc]


object MilanAggregateFunction {
  val typeName: String = getClass.getTypeName.stripSuffix("$")
}
