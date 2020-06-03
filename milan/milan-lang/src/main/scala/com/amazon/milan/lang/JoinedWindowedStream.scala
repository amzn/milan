package com.amazon.milan.lang

import com.amazon.milan.lang.internal.JoinedWindowedStreamMacros

import scala.language.experimental.macros


/**
 * Represents a left join operation between records in one stream windows of another stream.
 *
 * @param leftInput  The left input windowed stream.
 * @param rightInput The right input windowed stream.
 * @tparam TLeft  The type of records on the left stream.
 * @tparam TRight The type of records on the right stream.
 */
class LeftJoinedWindowedStream[TLeft, TRight](val leftInput: Stream[TLeft],
                                              val rightInput: WindowedStream[TRight]) {
  /**
   * Applies a function to produce outputs from the join.
   * The function is applied to each record from the left stream and the corresponding window from the right stream.
   *
   * @param applyFunction The function to apply.
   * @tparam TOut The output type of the function being applied.
   * @return A [[Stream]] representing the result of the operation.
   */
  def apply[TOut](applyFunction: (TLeft, Iterable[TRight]) => TOut): Stream[TOut] = macro JoinedWindowedStreamMacros.leftApply[TLeft, TRight, TOut]
}
