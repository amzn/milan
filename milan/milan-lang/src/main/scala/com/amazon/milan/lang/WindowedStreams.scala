package com.amazon.milan.lang

import java.time.Instant

import com.amazon.milan.lang.internal.WindowedStreamMacros
import com.amazon.milan.program.{GroupingExpression, StreamExpression}

import scala.language.experimental.macros


/**
 * Represents the result of a windowing operation where the windows are not grouped.
 *
 * @param expr The Milan expression representing windowing operation.
 * @tparam T The type of the stream.
 */
class WindowedStream[T](val expr: StreamExpression) extends UnkeyedGroupOperations[T] {
  /**
   * Gets a copy of this [[WindowedStream]] object with the specified name assigned.
   *
   * @param name The name to assign.
   * @return A copy of the stream with the specified name assigned.
   */
  def withName(name: String): WindowedStream[T] = {
    new WindowedStream[T](this.expr.withName(name))
  }

  /**
   * Gets a copy of this [[WindowedStream]] object with the specified ID assigned.
   *
   * @param id The ID to assign.
   * @return A copy of the stream with the specified ID assigned.
   */
  def withId(id: String): WindowedStream[T] = {
    new WindowedStream[T](this.expr.withId(id))
  }

  def apply[TOut](f: Iterable[T] => TOut): Stream[TOut] = macro WindowedStreamMacros.apply[T, TOut]
}


/**
 * Represents the result of a time windowing operation.
 *
 * @param expr The Milan expression representing windowing operation.
 * @tparam T The type of the stream.
 */
class TimeWindowedStream[T](val expr: GroupingExpression) extends KeyedGroupOperations[T, Instant] {
  /**
   * Gets a copy of this [[TimeWindowedStream]] object with the specified name assigned.
   *
   * @param name The name to assign.
   * @return A copy of the stream with the specified name assigned.
   */
  def withName(name: String): TimeWindowedStream[T] = {
    new TimeWindowedStream[T](this.expr.withName(name).asInstanceOf[GroupingExpression])
  }

  /**
   * Gets a copy of this [[TimeWindowedStream]] object with the specified ID assigned.
   *
   * @param id The ID to assign.
   * @return A copy of the stream with the specified ID assigned.
   */
  def withId(id: String): TimeWindowedStream[T] = {
    new TimeWindowedStream[T](this.expr.withId(id).asInstanceOf[GroupingExpression])
  }

  def apply[TOut](f: Iterable[T] => TOut): Stream[T] = throw new NotImplementedError()
}
