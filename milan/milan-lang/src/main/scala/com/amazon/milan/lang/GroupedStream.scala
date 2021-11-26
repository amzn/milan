package com.amazon.milan.lang

import com.amazon.milan.Id
import com.amazon.milan.lang.internal.{GroupedStreamMacros, StreamMacros}
import com.amazon.milan.program.{SlidingRecordWindow, StreamExpression}
import com.amazon.milan.typeutil.types

import java.time.{Duration, Instant}
import scala.language.experimental.macros


/**
 * Represents the result of a group by operation where the key is specified.
 *
 * @param expr The Milan expression representing the group by operation.
 * @tparam T    The type of the stream.
 * @tparam TKey The type of the group key.
 */
class GroupedStream[T, TKey](val expr: StreamExpression) extends KeyedGroupOperations[T, TKey] {
  /**
   * Gets a copy of this [[GroupedStream]] object with the specified name assigned.
   *
   * @param name The name to assign.
   * @return A copy of the stream with the specified name assigned.
   */
  def withName(name: String): GroupedStream[T, TKey] = {
    new GroupedStream[T, TKey](this.expr.withName(name))
  }

  /**
   * Gets a copy of this [[GroupedStream]] object with the specified ID assigned.
   *
   * @param id The ID to assign.
   * @return A copy of the stream with the specified ID assigned.
   */
  def withId(id: String): GroupedStream[T, TKey] = {
    new GroupedStream[T, TKey](this.expr.withId(id))
  }

  /**
   * Defines a window containing the latest records from every group.
   *
   * @param windowSize The window size.
   * @return A [[WindowedStream]] representing the result of the windowing operation.
   */
  def recordWindow(windowSize: Int): WindowedStream[T] = {
    val id = Id.newId()
    val outputExpr = new SlidingRecordWindow(this.expr, windowSize, id, id, this.expr.recordType.toGroupedStream(types.Long))
    new WindowedStream[T](outputExpr)
  }

  /**
   * Defines a tumbling window over a date/time that is extracted from stream records.
   *
   * @param dateExtractor A function that extracts a date/time from a record.
   * @param windowPeriod  The length of a window.
   * @param offset        By default windows are aligned with the epoch, 1970-01-01.
   *                      This offset shifts the window alignment to the specified duration after the epoch.
   * @return A [[TimeWindowedStream]] representing the result of the windowing operation.
   */
  def tumblingWindow(dateExtractor: T => Instant, windowPeriod: Duration, offset: Duration): TimeWindowedStream[T] = macro StreamMacros.tumblingWindow[T]

  /**
   * Defines a sliding window over a date/time that is extracted from stream records.
   *
   * @param dateExtractor A function that extracts a date/time from a record.
   * @param windowSize    The length of a window.
   * @param slide         The distance (in time) between window start times.
   * @param offset        By default windows are aligned with the epoch, 1970-01-01.
   *                      This offset shifts the window alignment to the specified duration after the epoch.
   * @return A [[TimeWindowedStream]] representing the result of the windowing operation.
   */
  def slidingWindow(dateExtractor: T => Instant, windowSize: Duration, slide: Duration, offset: Duration): TimeWindowedStream[T] = macro StreamMacros.slidingWindow[T]

  /**
   * Maps each stream of grouped records to another stream.
   *
   * @param f A function that maps each stream of grouped records to another stream.
   * @tparam TOut The output record type.
   * @return A [[GroupedStream]] containing the mapped groups.
   */
  def map[TOut](f: (TKey, Stream[T]) => Stream[TOut]): GroupedStream[TOut, TKey] = macro GroupedStreamMacros.map[T, TKey, TOut]
}


/**
 * Represents the result of a grouping operation.
 *
 * @param expr The expression representing the grouping operation.
 * @tparam T The type of stream records.
 */
class UnkeyedGroupedStream[T](val expr: StreamExpression) extends UnkeyedGroupOperations[T] {

}