package com.amazon.milan.flink.runtime

import com.amazon.milan.flink.TypeUtil
import com.amazon.milan.flink.api.MilanAggregateFunction
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.scala.createTypeInformation

object BuiltinAggregateFunctions {
  val typeName: String = getClass.getTypeName.stripSuffix("$")

  abstract class SimpleAggregateFunction[T](emptyAccumulator: Option[T],
                                            valueTypeInfo: TypeInformation[T])
    extends MilanAggregateFunction[T, Option[T], T] {

    protected def addValues(in: T, acc: T): T

    override def add(in: T, acc: Option[T]): Option[T] = {
      if (acc.isEmpty) {
        Some(in)
      }
      else {
        Some(this.addValues(in, acc.get))
      }
    }

    override def createAccumulator(): Option[T] = this.emptyAccumulator

    override def getResult(acc: Option[T]): T = acc.get

    override def merge(acc: Option[T], acc1: Option[T]): Option[T] = {
      if (acc.isEmpty && acc1.isEmpty) {
        None
      }
      else if (acc.isEmpty) {
        acc1
      }
      else if (acc1.isEmpty) {
        acc
      }
      else {
        this.add(acc.get, acc1)
      }
    }

    override def getProducedType: TypeInformation[T] =
      this.valueTypeInfo

    override def getAccumulatorType: TypeInformation[Option[T]] =
      TypeUtil.createOptionTypeInfo(this.valueTypeInfo)
  }


  abstract class NumericAggregateFunction[T: Numeric](emptyAccumulator: Option[T],
                                                      valueTypeInfo: TypeInformation[T])
    extends SimpleAggregateFunction[T](emptyAccumulator, valueTypeInfo) {

    def this(valueTypeInfo: TypeInformation[T]) {
      this(Some(implicitly[Numeric[T]].zero), valueTypeInfo)
    }

    @transient protected lazy val numeric: Numeric[T] = implicitly[Numeric[T]]
  }

  class Any[T](valueTypeInfo: TypeInformation[T])
    extends SimpleAggregateFunction[T](None, valueTypeInfo) {

    override def addValues(in: T, acc: T): T = in
  }

  class Sum[T: Numeric](valueTypeInfo: TypeInformation[T])
    extends NumericAggregateFunction[T](valueTypeInfo) {

    override def addValues(in: T, acc: T): T = this.numeric.plus(in, acc)
  }


  class Min[T: Numeric](valueTypeInfo: TypeInformation[T])
    extends NumericAggregateFunction[T](None, valueTypeInfo) {

    override def addValues(in: T, acc: T): T = this.numeric.min(in, acc)
  }


  class Max[T: Numeric](valueTypeInfo: TypeInformation[T])
    extends NumericAggregateFunction[T](None, valueTypeInfo) {

    override def addValues(in: T, acc: T): T = this.numeric.max(in, acc)
  }


  class Mean[T: Numeric](valueTypeInfo: TypeInformation[T])
    extends MilanAggregateFunction[T, (Long, T), Double] {

    @transient private lazy val numeric: Numeric[T] = implicitly[Numeric[T]]

    override def add(in: T, acc: (Long, T)): (Long, T) = {
      val (count, sum) = acc
      (count + 1, this.numeric.plus(in, sum))
    }

    override def createAccumulator(): (Long, T) =
      (0, this.numeric.zero)

    override def getResult(acc: (Long, T)): Double = {
      val (count, sum) = acc
      this.numeric.toDouble(sum) / count.toDouble
    }

    override def merge(acc: (Long, T), acc1: (Long, T)): (Long, T) = {
      val (count, sum) = acc
      val (count1, sum1) = acc1
      (count + count1, this.numeric.plus(sum, sum1))
    }

    override def getAccumulatorType: TypeInformation[(Long, T)] =
      TypeUtil.createTupleTypeInfo[(Long, T)](createTypeInformation[Long], this.valueTypeInfo)

    override def getProducedType: TypeInformation[Double] =
      createTypeInformation[Double]
  }


  abstract class ArgCompareAggregateFunction[TArg, T](argTypeInfo: TypeInformation[TArg],
                                                      valueTypeInfo: TypeInformation[T])
    extends MilanAggregateFunction[(TArg, T), (Option[TArg], Option[T]), T] {

    protected def checkReplace(in: TArg, current: TArg): Boolean

    override def add(in: (TArg, T), acc: (Option[TArg], Option[T])): (Option[TArg], Option[T]) = {
      val (arg, value) = in
      val (accArg, _) = acc
      if (accArg.isEmpty || this.checkReplace(arg, accArg.get)) {
        (Some(arg), Some(value))
      }
      else {
        acc
      }
    }

    override def createAccumulator(): (Option[TArg], Option[T]) = (None, None)

    override def getResult(acc: (Option[TArg], Option[T])): T = {
      val (_, value) = acc
      value.get
    }

    override def merge(acc: (Option[TArg], Option[T]),
                       acc1: (Option[TArg], Option[T])): (Option[TArg], Option[T]) = {
      val (arg, _) = acc
      val (arg1, _) = acc1
      if (arg.isEmpty) {
        acc1
      }
      else if (arg1.isEmpty) {
        acc
      }
      else if (this.checkReplace(arg.get, arg1.get)) {
        acc
      }
      else {
        acc1
      }
    }

    override def getProducedType: TypeInformation[T] =
      this.valueTypeInfo

    override def getAccumulatorType: TypeInformation[(Option[TArg], Option[T])] =
      TypeUtil.createTupleTypeInfo(
        TypeUtil.createOptionTypeInfo(this.argTypeInfo),
        TypeUtil.createOptionTypeInfo(this.valueTypeInfo))
  }


  class ArgMin[TArg: Ordering, T](argTypeInfo: TypeInformation[TArg],
                                  valueTypeInfo: TypeInformation[T])
    extends ArgCompareAggregateFunction[TArg, T](argTypeInfo, valueTypeInfo) {

    @transient private lazy val ordering = implicitly[Ordering[TArg]]

    override protected def checkReplace(in: TArg, current: TArg): Boolean = this.ordering.lt(in, current)
  }


  class ArgMax[TArg: Ordering, T](argTypeInfo: TypeInformation[TArg],
                                  valueTypeInfo: TypeInformation[T])
    extends ArgCompareAggregateFunction[TArg, T](argTypeInfo, valueTypeInfo) {

    @transient private lazy val ordering = implicitly[Ordering[TArg]]

    override protected def checkReplace(in: TArg, current: TArg): Boolean = this.ordering.gt(in, current)
  }


  /**
   * An aggregate function that does not perform any aggregation and just returns the last input value seen.
   * This is intended to be used for the portion of a user-specified aggregation function that only depends on the
   * group key and not on the input records.
   */
  class Constant[T](valueTypeInfo: TypeInformation[T])
    extends MilanAggregateFunction[T, Option[T], T] {

    override def add(in: T, acc: Option[T]): Option[T] = Some(in)

    override def createAccumulator(): Option[T] = None

    override def getResult(acc: Option[T]): T = acc.get

    override def merge(acc: Option[T], acc1: Option[T]): Option[T] = {
      if (acc.isEmpty) {
        acc1
      }
      else {
        acc
      }
    }

    override def getProducedType: TypeInformation[T] =
      this.valueTypeInfo

    override def getAccumulatorType: TypeInformation[Option[T]] =
      TypeUtil.createOptionTypeInfo[T](this.valueTypeInfo)
  }

  /**
   * An aggregate function that counts the input records.
   */
  class Count
    extends MilanAggregateFunction[Unit, Long, Long] {

    override def add(in: Unit, acc: Long): Long = acc + 1

    override def createAccumulator(): Long = 0

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1

    override def getProducedType: TypeInformation[Long] = BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]]

    override def getAccumulatorType: TypeInformation[Long] = BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]]
  }

}
