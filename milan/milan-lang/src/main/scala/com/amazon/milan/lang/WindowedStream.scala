package com.amazon.milan.lang

import java.time.Instant

import com.amazon.milan.lang.internal.WindowedStreamMacros
import com.amazon.milan.program.ComputedGraphNode

import scala.language.experimental.macros


/**
 * Represents the result of a windowing operation.
 *
 * @param node The graph node representing the windowing operation.
 * @tparam T       The type of the stream.
 * @tparam TStream The type of the stream object that was used to create the grouped stream.
 */
class WindowedStream[T, TStream <: Stream[T, _]](val node: ComputedGraphNode) extends GroupOperations[T, Instant, TStream] {
  /**
   * Specifies that for any following aggregate operations, only the latest record for each selector value will be
   * included in the aggregate calculation.
   *
   * @param selector A function that computes the value that must be unique in the group.
   * @tparam TVal The type of value computed.
   * @return A [[GroupedStream]] with the uniqueness constraint applied to each group.
   */
  def unique[TVal](selector: T => TVal): WindowedStream[T, TStream] = macro WindowedStreamMacros.unique[T, TVal, TStream]
}
