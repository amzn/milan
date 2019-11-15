package com.amazon.milan.lang


trait ReducingAggregateFunction {
  def apply[T](value: T): T = throw new NotImplementedError()
}


trait CoercingReducingAggregateFunction[TOut] {
  def apply[T](value: T): TOut = throw new NotImplementedError()
}


trait ArgReducingAggregateFunction {
  def apply[TKey, TValue](key: TKey, value: TValue): TValue = throw new NotImplementedError()
}


object aggregation {

  object sum extends ReducingAggregateFunction

  object min extends ReducingAggregateFunction

  object max extends ReducingAggregateFunction

  object mean extends CoercingReducingAggregateFunction[Double]

  object any extends ReducingAggregateFunction

  object argmin extends ArgReducingAggregateFunction

  object argmax extends ArgReducingAggregateFunction

}
