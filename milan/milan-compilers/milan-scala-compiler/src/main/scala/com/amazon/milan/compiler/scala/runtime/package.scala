package com.amazon.milan.compiler.scala

package object runtime {
  def maxBy[T, TArg: Ordering](stream: Stream[T], argExtractor: T => TArg): Stream[T] = {
    val ordering = implicitly[Ordering[TArg]]
    this.argMin(stream, argExtractor, (arg1, arg2) => ordering.gt(arg1, arg2))
  }

  def minBy[T, TArg: Ordering](stream: Stream[T], argExtractor: T => TArg): Stream[T] = {
    val ordering = implicitly[Ordering[TArg]]
    this.argMin(stream, argExtractor, (arg1, arg2) => ordering.lt(arg1, arg2))
  }

  def sumBy[T, TArg: Numeric, TOut](stream: Stream[T],
                                    argExtractor: T => TArg,
                                    getResult: (T, TArg) => TOut): Stream[TOut] = {
    val numeric = implicitly[Numeric[TArg]]

    this.reduce(
      stream,
      numeric.zero,
      argExtractor,
      numeric.plus,
      getResult)
  }

  private def argMin[T, TArg](stream: Stream[T], argExtractor: T => TArg, lessThan: (TArg, TArg) => Boolean): Stream[T] = {
    val initialState: (Option[TArg], Option[T]) = (None, None)

    stream
      .scanLeft(initialState)((state, value) => {
        val arg = argExtractor(value)

        state match {
          case (None, _) =>
            (Some(arg), Some(value))

          case (Some(maxArg), _) if lessThan(arg, maxArg) =>
            (Some(arg), Some(value))

          case (Some(maxArg), _) =>
            (Some(maxArg), None)
        }
      })
      .map { case (_, out) => out }
      .filter(_.isDefined)
      .map(_.get)
  }

  private def reduce[T, TState, TOut](stream: Stream[T],
                                      initialState: TState,
                                      stateExtractor: T => TState,
                                      stateCombiner: (TState, TState) => TState,
                                      getResult: (T, TState) => TOut): Stream[TOut] = {
    val initialScanState: (TState, Option[TOut]) = (initialState, None)

    stream
      .scanLeft(initialScanState)((scanState, value) => {
        val (state, _) = scanState
        val newState = stateCombiner(state, stateExtractor(value))
        val result = getResult(value, newState)
        (newState, Some(result))
      })
      .map { case (_, out) => out }
      .filter(_.isDefined)
      .map(_.get)
  }
}
