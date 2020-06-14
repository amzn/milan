package com.amazon.milan.compiler.flink.runtime

import com.amazon.milan.compiler.flink.TypeUtil
import com.amazon.milan.compiler.flink.types.{AggregatorOutputRecord, AggregatorOutputRecordTypeInformation, RecordWrapper}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable


class MultiAggregateFunction0[TIn >: Null, TKey >: Null <: Product]
  extends AggregateFunction[RecordWrapper[TIn, TKey], Product, AggregatorOutputRecord[Product]]
    with ResultTypeQueryable[AggregatorOutputRecord[Product]] {

  override def createAccumulator(): Product = None

  override def add(in: RecordWrapper[TIn, TKey], acc: Product): Product = acc

  override def getResult(acc: Product): AggregatorOutputRecord[Product] = AggregatorOutputRecord.wrap[Product](acc)

  override def merge(acc: Product, acc1: Product): Product = acc

  override def getProducedType: TypeInformation[AggregatorOutputRecord[Product]] =
    AggregatorOutputRecordTypeInformation.wrap(TypeUtil.createTupleTypeInfo[Product]())
}


class MultiAggregateFunction1[TIn >: Null, TKey >: Null <: Product, TAcc, TOut](agg: AggregateFunction[TIn, TAcc, TOut] with ResultTypeQueryable[TOut])
  extends AggregateFunction[RecordWrapper[TIn, TKey], Tuple1[TAcc], AggregatorOutputRecord[Tuple1[TOut]]]
    with ResultTypeQueryable[AggregatorOutputRecord[Tuple1[TOut]]] {

  override def add(in: RecordWrapper[TIn, TKey], acc: Tuple1[TAcc]): Tuple1[TAcc] = {
    Tuple1(this.agg.add(in.value, acc._1))
  }

  override def createAccumulator(): Tuple1[TAcc] = {
    Tuple1(this.agg.createAccumulator())
  }

  override def getResult(acc: Tuple1[TAcc]): AggregatorOutputRecord[Tuple1[TOut]] = {
    AggregatorOutputRecord.wrap(Tuple1(this.agg.getResult(acc._1)))
  }

  override def merge(acc: Tuple1[TAcc], acc1: Tuple1[TAcc]): Tuple1[TAcc] = {
    Tuple1(this.agg.merge(acc._1, acc1._1))
  }

  override def getProducedType: TypeInformation[AggregatorOutputRecord[Tuple1[TOut]]] =
    AggregatorOutputRecordTypeInformation.wrap(
      TypeUtil.createTupleTypeInfo[Tuple1[TOut]](this.agg.getProducedType))
}


class MultiAggregateFunction2[TIn >: Null, TKey >: Null <: Product, TAcc1, TOut1, TAcc2, TOut2](agg1: AggregateFunction[TIn, TAcc1, TOut1] with ResultTypeQueryable[TOut1],
                                                                                                agg2: AggregateFunction[TIn, TAcc2, TOut2] with ResultTypeQueryable[TOut2])
  extends AggregateFunction[RecordWrapper[TIn, TKey], (TAcc1, TAcc2), AggregatorOutputRecord[(TOut1, TOut2)]]
    with ResultTypeQueryable[AggregatorOutputRecord[(TOut1, TOut2)]] {

  override def add(in: RecordWrapper[TIn, TKey], acc: (TAcc1, TAcc2)): (TAcc1, TAcc2) = {
    val (acc1, acc2) = acc
    (this.agg1.add(in.value, acc1), this.agg2.add(in.value, acc2))
  }

  override def createAccumulator(): (TAcc1, TAcc2) = {
    (this.agg1.createAccumulator(), this.agg2.createAccumulator())
  }

  override def getResult(acc: (TAcc1, TAcc2)): AggregatorOutputRecord[(TOut1, TOut2)] = {
    val (acc1, acc2) = acc
    AggregatorOutputRecord.wrap((this.agg1.getResult(acc1), this.agg2.getResult(acc2)))
  }

  override def merge(accA: (TAcc1, TAcc2), accB: (TAcc1, TAcc2)): (TAcc1, TAcc2) = {
    val (a1, a2) = accA
    val (b1, b2) = accB
    (this.agg1.merge(a1, b1), this.agg2.merge(a2, b2))
  }

  override def getProducedType: TypeInformation[AggregatorOutputRecord[(TOut1, TOut2)]] =
    AggregatorOutputRecordTypeInformation.wrap(
      TypeUtil.createTupleTypeInfo[(TOut1, TOut2)](
        this.agg1.getProducedType,
        this.agg2.getProducedType))
}


class MultiAggregateFunction3[TIn >: Null, TKey >: Null <: Product, TAcc1, TOut1, TAcc2, TOut2, TAcc3, TOut3](agg1: AggregateFunction[TIn, TAcc1, TOut1] with ResultTypeQueryable[TOut1],
                                                                                                              agg2: AggregateFunction[TIn, TAcc2, TOut2] with ResultTypeQueryable[TOut2],
                                                                                                              agg3: AggregateFunction[TIn, TAcc3, TOut3] with ResultTypeQueryable[TOut3])
  extends AggregateFunction[RecordWrapper[TIn, TKey], (TAcc1, TAcc2, TAcc3), AggregatorOutputRecord[(TOut1, TOut2, TOut3)]]
    with ResultTypeQueryable[AggregatorOutputRecord[(TOut1, TOut2, TOut3)]] {

  override def add(in: RecordWrapper[TIn, TKey], acc: (TAcc1, TAcc2, TAcc3)): (TAcc1, TAcc2, TAcc3) = {
    val (acc1, acc2, acc3) = acc
    (this.agg1.add(in.value, acc1), this.agg2.add(in.value, acc2), this.agg3.add(in.value, acc3))
  }

  override def createAccumulator(): (TAcc1, TAcc2, TAcc3) = {
    (this.agg1.createAccumulator(), this.agg2.createAccumulator(), this.agg3.createAccumulator())
  }

  override def getResult(acc: (TAcc1, TAcc2, TAcc3)): AggregatorOutputRecord[(TOut1, TOut2, TOut3)] = {
    val (acc1, acc2, acc3) = acc
    AggregatorOutputRecord.wrap((this.agg1.getResult(acc1), this.agg2.getResult(acc2), this.agg3.getResult(acc3)))
  }

  override def merge(accA: (TAcc1, TAcc2, TAcc3), accB: (TAcc1, TAcc2, TAcc3)): (TAcc1, TAcc2, TAcc3) = {
    val (a1, a2, a3) = accA
    val (b1, b2, b3) = accB
    (this.agg1.merge(a1, b1), this.agg2.merge(a2, b2), this.agg3.merge(a3, b3))
  }

  override def getProducedType: TypeInformation[AggregatorOutputRecord[(TOut1, TOut2, TOut3)]] =
    AggregatorOutputRecordTypeInformation.wrap(
      TypeUtil.createTupleTypeInfo[(TOut1, TOut2, TOut3)](
        this.agg1.getProducedType,
        this.agg2.getProducedType,
        this.agg3.getProducedType))
}
