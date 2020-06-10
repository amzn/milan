package com.amazon.milan.lang

import com.amazon.milan.program.Cycle
import com.amazon.milan.typeutil.TypeDescriptor

import scala.language.experimental.macros


/**
 * A stream that can be used to create a cycle in the streaming graph.
 *
 * @param expr       The [[Cycle]] expression that defines the stream.
 * @param recordType The record type of the stream.
 * @tparam T The record type.
 */
class CycleStream[T](expr: Cycle, recordType: TypeDescriptor[T])
  extends Stream[T](expr, recordType) {

  /**
   * Sets the forward reference to the specified stream.
   */
  def closeCycle(other: Stream[T]): Unit = {
    this.expr.cycleNodeId = other.streamId
  }
}
