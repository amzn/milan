package com.amazon.milan.lang

import java.time.{Duration, Instant}

import com.amazon.milan.lang.internal.{GroupedStreamMacros, StreamMacros}
import com.amazon.milan.program.StreamExpression

import scala.language.experimental.macros


/**
 * Represents the result of a group by operation.
 *
 * @param expr The Milan expression representing the group by operation.
 * @tparam T    The type of the stream.
 * @tparam TKey The type of the group key.
 */
class GroupedStream[T, TKey](val expr: StreamExpression) extends GroupOperations[T, TKey] {
  /**
   * Specifies that for any following aggregate operations, only the latest record for each selector value will be
   * included in the aggregate calculation.
   *
   * @param selector A function that computes the value that must be unique in the group.
   * @tparam TVal The type of value computed.
   * @return A [[GroupedStream]] with the uniqueness constraint applied to each group.
   */
  def unique[TVal](selector: T => TVal): GroupedStream[T, TKey] = macro GroupedStreamMacros.unique[T, TKey, TVal]

  /**
   * Defines a tumbling window over a date/time that is extracted from stream records.
   *
   * @param dateExtractor A function that extracts a date/time from a record.
   * @param windowPeriod  The length of a window.
   * @param offset        By default windows are aligned with the epoch, 1970-01-01.
   *                      This offset shifts the window alignment to the specified duration after the epoch.
   * @return A [[WindowedStream]] representing the result of the windowing operation.
   */
  def tumblingWindow(dateExtractor: T => Instant, windowPeriod: Duration, offset: Duration): WindowedStream[T] = macro StreamMacros.tumblingWindow[T]

  /**
   * Defines a sliding window over a date/time that is extracted from stream records.
   *
   * @param dateExtractor A function that extracts a date/time from a record.
   * @param windowSize    The length of a window.
   * @param slide         The distance (in time) between window start times.
   * @param offset        By default windows are aligned with the epoch, 1970-01-01.
   *                      This offset shifts the window alignment to the specified duration after the epoch.
   * @return A [[WindowedStream]] representing the result of the windowing operation.
   */
  def slidingWindow(dateExtractor: T => Instant, windowSize: Duration, slide: Duration, offset: Duration): WindowedStream[T] = macro StreamMacros.slidingWindow[T]

  /**
   * Maps each stream of grouped records to another stream.
   *
   * @param f A function that maps each stream of grouped records to another stream.
   * @tparam TOut The output record type.
   * @return A [[GroupedStream]] containing the mapped groups.
   */
  def map[TOut](f: (TKey, Stream[T]) => Stream[TOut]): GroupedStream[TOut, TKey] = macro GroupedStreamMacros.map[T, TKey, TOut]

  /**
   * Maps each stream of grouped records to another stream, and combines all output streams into a single stream.
   *
   * @param f A function that maps each stream of grouped records to another stream.
   * @tparam TOut The output record type.
   * @return A [[Stream]] containing all of the output records from all groups.
   */
  def flatMap[TOut](f: (TKey, Stream[T]) => Stream[TOut]): Stream[TOut] = macro GroupedStreamMacros.flatMap[T, TKey, TOut]
}
