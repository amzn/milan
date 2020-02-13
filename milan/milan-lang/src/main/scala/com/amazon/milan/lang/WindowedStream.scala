package com.amazon.milan.lang

import java.time.Instant

import com.amazon.milan.lang.internal.WindowedStreamMacros
import com.amazon.milan.program.GroupingExpression

import scala.language.experimental.macros


/**
 * Represents the result of a windowing operation.
 *
 * @param expr The Milan expression representing windowing operation.
 * @tparam T The type of the stream.
 */
class WindowedStream[T](val expr: GroupingExpression) extends GroupOperations[T, Instant] {
  /**
   * Specifies that for any following aggregate operations, only the latest record for each selector value will be
   * included in the aggregate calculation.
   *
   * @param selector A function that computes the value that must be unique in the group.
   * @tparam TVal The type of value computed.
   * @return A [[GroupedStream]] with the uniqueness constraint applied to each group.
   */
  def unique[TVal](selector: T => TVal): WindowedStream[T] = macro WindowedStreamMacros.unique[T, TVal]
}
