package com.amazon.milan.flink.components

import com.amazon.milan.flink.api.MilanAggregateFunction
import com.amazon.milan.flink.{RuntimeEvaluator, TypeUtil}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple0, Tuple1, Tuple2, Tuple3}


class MultiAggregateFunction0[TIn](aggregateFunctions: List[MilanAggregateFunction[_, _, _]])
  extends MilanAggregateFunction[TIn, Tuple0, Tuple0] {

  assert(aggregateFunctions.isEmpty)

  override def createAccumulator(): Tuple0 = new Tuple0()

  override def add(in: TIn, acc: Tuple0): Tuple0 = acc

  override def getResult(acc: Tuple0): Tuple0 = acc

  override def merge(acc: Tuple0, acc1: Tuple0): Tuple0 = acc

  override def getProducedType: TypeInformation[Tuple0] = TypeUtil.createTupleTypeInfo[Tuple0]()

  override def getAccumulatorType: TypeInformation[Tuple0] = TypeUtil.createTupleTypeInfo[Tuple0]()
}


class MultiAggregateFunction1[TIn, TAcc, TOut](agg: MilanAggregateFunction[TIn, TAcc, TOut])
  extends MilanAggregateFunction[TIn, Tuple1[TAcc], Tuple1[TOut]] {

  def this(aggregateFunctions: List[MilanAggregateFunction[_, _, _]]) {
    this(aggregateFunctions.head.asInstanceOf[MilanAggregateFunction[TIn, TAcc, TOut]])
    assert(aggregateFunctions.length == 1)
  }

  override def add(in: TIn, acc: Tuple1[TAcc]): Tuple1[TAcc] = {
    new Tuple1(this.agg.add(in, acc.f0))
  }

  override def createAccumulator(): Tuple1[TAcc] = {
    new Tuple1(this.agg.createAccumulator())
  }

  override def getResult(acc: Tuple1[TAcc]): Tuple1[TOut] = {
    new Tuple1(this.agg.getResult(acc.f0))
  }

  override def merge(acc: Tuple1[TAcc], acc1: Tuple1[TAcc]): Tuple1[TAcc] = {
    new Tuple1(this.agg.merge(acc.f0, acc1.f0))
  }

  override def getProducedType: TypeInformation[Tuple1[TOut]] =
    TypeUtil.createTupleTypeInfo[Tuple1[TOut]](this.agg.getProducedType)

  override def getAccumulatorType: TypeInformation[Tuple1[TAcc]] =
    TypeUtil.createTupleTypeInfo[Tuple1[TAcc]](this.agg.getAccumulatorType)
}


class MultiAggregateFunction2[TIn, TAcc1, TOut1, TAcc2, TOut2](agg1: MilanAggregateFunction[TIn, TAcc1, TOut1],
                                                               agg2: MilanAggregateFunction[TIn, TAcc2, TOut2])
  extends MilanAggregateFunction[TIn, Tuple2[TAcc1, TAcc2], Tuple2[TOut1, TOut2]] {

  def this(aggregateFunctions: List[MilanAggregateFunction[_, _, _]]) {
    this(
      aggregateFunctions.head.asInstanceOf[MilanAggregateFunction[TIn, TAcc1, TOut1]],
      aggregateFunctions(1).asInstanceOf[MilanAggregateFunction[TIn, TAcc2, TOut2]])
    assert(aggregateFunctions.length == 2)
  }

  override def add(in: TIn, acc: Tuple2[TAcc1, TAcc2]): Tuple2[TAcc1, TAcc2] = {
    new Tuple2(this.agg1.add(in, acc.f0), this.agg2.add(in, acc.f1))
  }

  override def createAccumulator(): Tuple2[TAcc1, TAcc2] = {
    new Tuple2(this.agg1.createAccumulator(), this.agg2.createAccumulator())
  }

  override def getResult(acc: Tuple2[TAcc1, TAcc2]): Tuple2[TOut1, TOut2] = {
    new Tuple2(this.agg1.getResult(acc.f0), this.agg2.getResult(acc.f1))
  }

  override def merge(acc: Tuple2[TAcc1, TAcc2], acc1: Tuple2[TAcc1, TAcc2]): Tuple2[TAcc1, TAcc2] = {
    new Tuple2(this.agg1.merge(acc.f0, acc1.f0), this.agg2.merge(acc.f1, acc1.f1))
  }

  override def getProducedType: TypeInformation[Tuple2[TOut1, TOut2]] =
    TypeUtil.createTupleTypeInfo[Tuple2[TOut1, TOut2]](
      this.agg1.getProducedType,
      this.agg2.getProducedType)

  override def getAccumulatorType: TypeInformation[Tuple2[TAcc1, TAcc2]] =
    TypeUtil.createTupleTypeInfo[Tuple2[TAcc1, TAcc2]](
      this.agg1.getAccumulatorType,
      this.agg2.getAccumulatorType)
}


class MultiAggregateFunction3[TIn, TAcc1, TOut1, TAcc2, TOut2, TAcc3, TOut3](agg1: MilanAggregateFunction[TIn, TAcc1, TOut1],
                                                                             agg2: MilanAggregateFunction[TIn, TAcc2, TOut2],
                                                                             agg3: MilanAggregateFunction[TIn, TAcc3, TOut3])
  extends MilanAggregateFunction[TIn, Tuple3[TAcc1, TAcc2, TAcc3], Tuple3[TOut1, TOut2, TOut3]] {

  def this(aggregateFunctions: List[MilanAggregateFunction[_, _, _]]) {
    this(
      aggregateFunctions.head.asInstanceOf[MilanAggregateFunction[TIn, TAcc1, TOut1]],
      aggregateFunctions(1).asInstanceOf[MilanAggregateFunction[TIn, TAcc2, TOut2]],
      aggregateFunctions(2).asInstanceOf[MilanAggregateFunction[TIn, TAcc3, TOut3]])
    assert(aggregateFunctions.length == 3)
  }

  override def add(in: TIn, acc: Tuple3[TAcc1, TAcc2, TAcc3]): Tuple3[TAcc1, TAcc2, TAcc3] = {
    new Tuple3(this.agg1.add(in, acc.f0), this.agg2.add(in, acc.f1), this.agg3.add(in, acc.f2))
  }

  override def createAccumulator(): Tuple3[TAcc1, TAcc2, TAcc3] = {
    new Tuple3(this.agg1.createAccumulator(), this.agg2.createAccumulator(), this.agg3.createAccumulator())
  }

  override def getResult(acc: Tuple3[TAcc1, TAcc2, TAcc3]): Tuple3[TOut1, TOut2, TOut3] = {
    new Tuple3(this.agg1.getResult(acc.f0), this.agg2.getResult(acc.f1), this.agg3.getResult(acc.f2))
  }

  override def merge(acc: Tuple3[TAcc1, TAcc2, TAcc3], acc1: Tuple3[TAcc1, TAcc2, TAcc3]): Tuple3[TAcc1, TAcc2, TAcc3] = {
    new Tuple3(this.agg1.merge(acc.f0, acc1.f0), this.agg2.merge(acc.f1, acc1.f1), this.agg3.merge(acc.f2, acc1.f2))
  }

  override def getProducedType: TypeInformation[Tuple3[TOut1, TOut2, TOut3]] =
    TypeUtil.createTupleTypeInfo[Tuple3[TOut1, TOut2, TOut3]](
      this.agg1.getProducedType,
      this.agg2.getProducedType,
      this.agg3.getProducedType)

  override def getAccumulatorType: TypeInformation[Tuple3[TAcc1, TAcc2, TAcc3]] =
    TypeUtil.createTupleTypeInfo[Tuple3[TAcc1, TAcc2, TAcc3]](
      this.agg1.getAccumulatorType,
      this.agg2.getAccumulatorType,
      this.agg3.getAccumulatorType)
}


object MultiAggregateFunction {
  val typeName: String = getClass.getTypeName.stripSuffix("$")

  /**
   * Combines multiple [[AggregateFunction]] instances that all have the same input type.
   */
  def combineAggregateFunctions[TIn](aggregateFunctions: List[MilanAggregateFunction[TIn, _, _]],
                                     inputTypeName: String): MilanAggregateFunction[TIn, _, _] = {
    val inputCount = aggregateFunctions.length

    // Each input aggregator has two unique type arguments associated with it, and we need to pass these through to the
    // MultiAggregateFunction that we create.
    val inputAggregateFunctionTypeArgs = aggregateFunctions.map(f =>
      s"${TypeUtil.getTypeName(f.getAccumulatorType)}, ${TypeUtil.getTypeName(f.getProducedType)}")

    val multiAggregateTypeArgs = if (inputAggregateFunctionTypeArgs.isEmpty) "" else inputAggregateFunctionTypeArgs.mkString(", ", ", ", "")
    val multiAggregateTypeName = s"${this.typeName}$inputCount[$inputTypeName$multiAggregateTypeArgs]"

    val eval = RuntimeEvaluator.instance

    eval.evalFunction[List[MilanAggregateFunction[TIn, _, _]], MilanAggregateFunction[TIn, _, _]](
      "aggregateFunctions",
      s"List[${MilanAggregateFunction.typeName}[$inputTypeName, _, _]]",
      s"new $multiAggregateTypeName(aggregateFunctions)",
      aggregateFunctions)
  }
}
