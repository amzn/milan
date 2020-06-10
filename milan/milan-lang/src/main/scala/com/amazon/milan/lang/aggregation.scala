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


/**
 * Methods that can be used inside a select() function.
 * select() is funny because the arguments to the select function have the type of individual records, but the operation
 * it performs is an aggregate operation.
 *
 * When the functions in the [[aggregation]] module are used, they tell the Milan Scala DSL that an aggregate operation
 * is being performed on the arguments of that function.
 *
 * None of these functions need to actually return a value because they are never invoked.
 */
object aggregation {

  object count {
    def apply(): Long = throw new NotImplementedError()
  }

  object sum extends ReducingAggregateFunction

  object min extends ReducingAggregateFunction

  object max extends ReducingAggregateFunction

  object mean extends CoercingReducingAggregateFunction[Double]

  object any extends ReducingAggregateFunction

  object argmin extends ArgReducingAggregateFunction

  object argmax extends ArgReducingAggregateFunction

}
