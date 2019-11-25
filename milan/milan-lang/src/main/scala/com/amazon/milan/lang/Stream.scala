package com.amazon.milan.lang

import java.time.{Duration, Instant}

import com.amazon.milan.lang.internal.StreamMacros
import com.amazon.milan.program
import com.amazon.milan.types.Record
import com.amazon.milan.typeutil.TypeDescriptor

import scala.language.experimental.macros


/**
 * Base class for streams.
 *
 * @param node The graph node representing the stream.
 * @tparam T The type of records on the stream.
 */
abstract class Stream[T, TStream <: Stream[T, _]](val node: program.Stream) {
  type RecordType = T

  def streamId: String = node.nodeId

  def streamName: String = node.name

  /**
   * Gets a [[TypeDescriptor]] describing the type of records on the stream.
   *
   * @return A [[TypeDescriptor]] for the stream records, or null if the type cannot be determined.
   */
  def getRecordType: TypeDescriptor[T] =
    this.node.getStreamExpression.tpe match {
      case null => null
      case ty => ty.asStream.recordType.asInstanceOf[TypeDescriptor[T]]
    }

  /**
   * Gets a copy of this [[Stream]] object with the specified name assigned.
   *
   * @param name The name to assign.
   * @return A copy of the stream with the specified name assigned.
   */
  def withName(name: String): Stream[T, TStream]

  /**
   * Gets a copy of this [[Stream]] object with the specified ID assigned.
   *
   * @param id The ID to assign.
   * @return A copy of the stream with the specified ID assigned.
   */
  def withId(id: String): Stream[T, TStream]

  /**
   * Defines a map relationship between this [[Stream]] and an [[ObjectStream]].
   *
   * @param f The map function.
   * @tparam TOut The type of the output objects.
   * @return An [[ObjectStream]]`[`[[TOut]]`]` representing the output mapped stream.
   */
  def map[TOut <: Record](f: T => TOut): ObjectStream[TOut] = macro StreamMacros.map[T, TOut]

  /**
   * Defines a map relationship between this [[Stream]] and a [[TupleStream]] stream with two fields.
   *
   * @return A [[TupleStream]] representing the output mapped stream.
   */
  def map[TF1, TF2](f1: FieldStatement[T, TF1], f2: FieldStatement[T, TF2]): TupleStream[(TF1, TF2)] = macro StreamMacros.mapTuple2[T, TF1, TF2]

  /**
   * Defines a "full outer join" relationship between this stream and another stream.
   *
   * @param other Another stream.
   * @tparam TOther The type of the other stream.
   * @return A [[JoinedStream]] representing the joined streams.
   * @note In a full outer join, an output record is produced any time an input record arrives on either stream.
   *       If no record on the other stream is available at that time, the corresponding field in the output stream
   *       will be empty.
   */
  def fullJoin[TOther](other: Stream[TOther, _]): JoinedStream[T, TOther] = {
    new JoinedStream[T, TOther](this.node, other.node, JoinType.FullEnrichmentJoin)
  }

  /**
   * Defines a "left outer join" relationship between this stream and another stream.
   *
   * @param other Another stream.
   * @tparam TOther The type of the other stream.
   * @return A [[JoinedStream]] representing the joined streams.
   * @note In a left outer join, an output record is produced any time an input record arrives on the left stream.
   *       If no record on the right stream is available at that time, the corresponding field in the output stream
   *       will be empty.
   */
  def leftJoin[TOther](other: Stream[TOther, _]): JoinedStream[T, TOther] = {
    new JoinedStream[T, TOther](this.node, other.node, JoinType.LeftEnrichmentJoin)
  }

  /**
   * Defines a "left outer join" relationship between this stream and a windowed stream.
   *
   * @param other The windowed stream being joined.
   * @tparam TOther The record type of the windowed stream.
   * @return A [[LeftJoinedWindowedStream]] representing the joined streams.
   */
  def leftJoin[TOther](other: WindowedStream[TOther, _]): LeftJoinedWindowedStream[T, TOther] = {
    new LeftJoinedWindowedStream[T, TOther](this, other)
  }

  /**
   * Defines a stream of a single window that always contains the latest record to arrive for every value of a key.
   *
   * @param dateExtractor A function that extracts a timestamp from input records, which is used to determine which
   *                      record is the latest (most recent).
   * @param keyFunc       A function that extracts a key from input records.
   * @return A [[WindowedStream]] representing the result of the windowing operation.
   * @todo Decide whether this makes sense as a language feature. It feels like join(latestBy).apply is a very specific
   *       construct that ought to be able to be written using more general language primitives. The compiler should
   *       then figure out the best way to execute it.
   */
  def latestBy[TKey](dateExtractor: T => Instant, keyFunc: T => TKey): WindowedStream[T, TStream] = macro StreamMacros.latestBy[T, TKey, TStream]

  /**
   * Defines a grouping over records in the stream.
   *
   * @param keyFunc A function that computes the group key for a record.
   * @tparam TKey The type of group key.
   * @return A [[GroupedStream]] representing the result of the grouping operation.
   */
  def groupBy[TKey](keyFunc: T => TKey): GroupedStream[T, TKey, TStream] = macro StreamMacros.groupBy[T, TKey, TStream]

  /**
   * Defines a stream of tumbling windows over a date/time that is extracted from stream records.
   *
   * @param dateExtractor A function that extracts a date/time from a record.
   * @param windowPeriod  The length of a window.
   * @param offset        By default windows are aligned with the epoch, 1970-01-01.
   *                      This offset shifts the window alignment to the specified duration after the epoch.
   * @return A [[WindowedStream]] representing the result of the windowing operation.
   */
  def tumblingWindow(dateExtractor: T => Instant, windowPeriod: Duration, offset: Duration): WindowedStream[T, TStream] = macro StreamMacros.tumblingWindow[T, TStream]

  /**
   * Defines a stream of sliding windows over a date/time that is extracted from stream records.
   *
   * @param dateExtractor A function that extracts a date/time from a record.
   * @param windowSize    The length of a window.
   * @param slide         The distance (in time) between window start times.
   * @param offset        By default windows are aligned with the epoch, 1970-01-01.
   *                      This offset shifts the window alignment to the specified duration after the epoch.
   * @return A [[WindowedStream]] representing the result of the windowing operation.
   */
  def slidingWindow(dateExtractor: T => Instant, windowSize: Duration, slide: Duration, offset: Duration): WindowedStream[T, TStream] = macro StreamMacros.slidingWindow[T, TStream]
}


object Stream {
  /**
   * Creates an [[ObjectStream]]`[`[[T]]`]` representing a stream of objects of the specified type.
   *
   * @tparam T The type of objects.
   * @return An [[ObjectStream]]`[`[[T]]`]` representing the stream.
   */
  def of[T <: Record]: ObjectStream[T] = macro StreamMacros.of[T]

  /**
   * Creates a [[TupleStream]]`[`[[T]]`]` representing a stream of named tuples of the specified type.
   *
   * @param fieldNames The field names corresponding to the tuple type parameters.
   * @tparam T The type of tuple objects.
   * @return A [[TupleStream]]`[`[[T]]`]` representing the stream.
   */
  def ofFields[T <: Product](fieldNames: String*): TupleStream[T] = macro StreamMacros.ofFields[T]
}


object JoinType extends Enumeration {
  type JoinType = Value

  val LeftEnrichmentJoin, FullEnrichmentJoin = Value
}
