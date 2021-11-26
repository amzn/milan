package com.amazon.milan.compiler.scala.event.operators

import com.amazon.milan.typeutil.{TypeDescriptor, types}


abstract class MinScanOperation[TIn, TKey, T: Ordering](valueType: TypeDescriptor[T])
  extends ScanOperationBase[TIn, TKey, Option[T], T](valueType.toOption, valueType) {

  override val initialState: Option[T] = None

  private lazy val ordering = implicitly[Ordering[T]]

  protected def getValue(input: TIn): T

  override def process(state: Option[T], input: TIn, key: TKey): (Option[T], T) = {
    val value = this.getValue(input)

    state match {
      case Some(stateValue) =>
        if (ordering.lt(value, stateValue)) {
          (Some(value), value)
        }
        else {
          (state, stateValue)
        }

      case None =>
        (Some(value), value)
    }
  }
}


abstract class MaxScanOperation[TIn, TKey, T: Ordering](valueType: TypeDescriptor[T])
  extends ScanOperationBase[TIn, TKey, Option[T], T](valueType.toOption, valueType) {

  override val initialState: Option[T] = None

  private lazy val ordering = implicitly[Ordering[T]]

  protected def getValue(input: TIn): T

  override def process(state: Option[T], input: TIn, key: TKey): (Option[T], T) = {
    val value = this.getValue(input)

    state match {
      case Some(stateValue) =>
        if (ordering.gt(value, stateValue)) {
          (Some(value), value)
        }
        else {
          (state, stateValue)
        }

      case None =>
        (Some(value), value)
    }
  }
}


abstract class SumScanOperation[TIn, TKey, T: Numeric](valueType: TypeDescriptor[T])
  extends ScanOperationBase[TIn, TKey, T, T](valueType, valueType) {

  private lazy val numeric = implicitly[Numeric[T]]

  override val initialState: T = this.numeric.zero

  protected def getValue(input: TIn): T

  override def process(state: T, input: TIn, key: TKey): (T, T) = {
    val value = this.getValue(input)
    val sum = numeric.plus(state, value)
    (sum, sum)
  }
}


abstract class CountScanOperation[TIn, TKey]
  extends ScanOperationBase[TIn, TKey, Long, Long](types.Long, types.Long) {

  override val initialState: Long = 0L

  override def process(state: Long, input: TIn, key: TKey): (Long, Long) = {
    val count = state + 1
    (count, count)
  }
}


abstract class FirstScanOperation[TIn, TKey, TOut](outputType: TypeDescriptor[TOut])
  extends ScanOperationBase[TIn, TKey, Option[TOut], TOut](outputType.toOption, outputType) {

  override val initialState: Option[TOut] = None

  protected def getValue(input: TIn): TOut

  override def process(state: Option[TOut], input: TIn, key: TKey): (Option[TOut], TOut) = {
    state match {
      case Some(value) =>
        (state, value)

      case None =>
        val value = this.getValue(input)
        (Some(value), value)
    }
  }
}
