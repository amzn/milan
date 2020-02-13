package com.amazon.milan.lang

import java.time.{Duration, Instant}

import com.amazon.milan.Id
import com.amazon.milan.lang.internal.StreamMacros
import com.amazon.milan.program.{FieldDefinition, FunctionDef, MapFields, SelectTerm, StreamExpression}
import com.amazon.milan.typeutil.{DataStreamTypeDescriptor, FieldDescriptor, TupleTypeDescriptor, TypeDescriptor, TypeJoiner}

import scala.language.experimental.macros


/**
 * Class for streams of records.
 *
 * @param expr       The Milan expression representing the stream.
 * @param recordType A [[TypeDescriptor]] describing the stream records.
 * @tparam T The type of records on the stream.
 */
class Stream[T](val expr: StreamExpression, val recordType: TypeDescriptor[T]) {
  type RecordType = T

  def streamId: String = expr.nodeId

  def streamName: String = expr.nodeName

  /**
   * Gets a copy of this [[Stream]] object with the specified name assigned.
   *
   * @param name The name to assign.
   * @return A copy of the stream with the specified name assigned.
   */
  def withName(name: String): Stream[T] = {
    new Stream[T](this.expr.withName(name), this.recordType)
  }

  /**
   * Gets a copy of this [[Stream]] object with the specified ID assigned.
   *
   * @param id The ID to assign.
   * @return A copy of the stream with the specified ID assigned.
   */
  def withId(id: String): Stream[T] = {
    new Stream[T](this.expr.withId(id), this.recordType)
  }

  /**
   * Define a filter relationship between this [[Stream]] and another [[Stream]] of the same type.
   *
   * @param predicate A predicate that indicates which records from this stream are present on the output stream.
   * @return A [[Stream]] representing the output filtered stream.
   */
  def where(predicate: T => Boolean): Stream[T] = macro StreamMacros.where[T]

  /**
   * Defines a map relationship between this [[Stream]] and an [[Stream]].
   *
   * @param f The map function.
   * @tparam TOut The type of the output objects.
   * @return An [[Stream]]`[`[[TOut]]`]` representing the output mapped stream.
   */
  def map[TOut](f: T => TOut): Stream[TOut] = macro StreamMacros.map[T, TOut]

  /**
   * Define a map relationship between this [[Stream]] and a stream with one field.
   *
   * @param f A function that maps the objects on this stream to a named field.
   * @tparam TF The type of the field.
   * @return An [[Stream]] representing the output mapped stream.
   */
  def map[TF](f: FieldStatement[T, TF]): Stream[Tuple1[TF]] = macro StreamMacros.mapTuple1[T, TF]

  /**
   * Defines a map relationship between this [[Stream]] and a [[Stream]] stream with two fields.
   *
   * @return A [[Stream]] representing the output mapped stream.
   */
  def map[TF1, TF2](f1: FieldStatement[T, TF1], f2: FieldStatement[T, TF2]): Stream[(TF1, TF2)] = macro StreamMacros.mapTuple2[T, TF1, TF2]

  /**
   * Converts this [[Stream]] to a [[Stream]] with one field whose values contain the records from
   * this stream.
   *
   * @param fieldName The name of the field to use in the output stream.
   * @return A [[Stream]] with one field with the specified name.
   */
  def toField(fieldName: String): Stream[Tuple1[T]] = {
    // Create a MapFields expression that maps the input records into a single field.
    val fieldDef = FieldDefinition(fieldName, FunctionDef(List("r"), SelectTerm("r")))
    val fieldDescriptor = FieldDescriptor(fieldName, this.recordType)
    val recordType = new TupleTypeDescriptor[Tuple1[T]](List(fieldDescriptor))

    val id = Id.newId()
    val outputType = new DataStreamTypeDescriptor(recordType)
    val mapExpr = new MapFields(this.expr, List(fieldDef), id, id, outputType)
    new Stream[Tuple1[T]](mapExpr, recordType)
  }

  /**
   * Adds a field to the current stream that is computed from record values.
   *
   * @param f      A [[FieldStatement]] that describes the new field.
   * @param joiner A [[TypeJoiner]] that can produce the output stream type.
   * @tparam TF The type of the new field.
   * @return A [[Stream]] containing the input records with the new field appended.
   */
  def addField[TF](f: FieldStatement[T, TF])(implicit joiner: TypeJoiner[T, Tuple1[TF]]): Stream[joiner.OutputType] = macro StreamMacros.addField[T, TF]

  /**
   * Adds fields to the current stream that are computed from record values.
   *
   * @param f1     A [[FieldStatement]] that describes the first new field.
   * @param f2     A [[FieldStatement]] that describes the second new field.
   * @param joiner A [[TypeJoiner]] that can produce the output stream type.
   * @tparam TF1 The type of the first new field.
   * @tparam TF2 The type of the second new field.
   * @return A [[Stream]] containing the input records with the new fields appended.
   */
  def addFields[TF1, TF2](f1: FieldStatement[T, TF1],
                          f2: FieldStatement[T, TF2])
                         (implicit joiner: TypeJoiner[T, (TF1, TF2)]): Stream[joiner.OutputType] = macro StreamMacros.addFields2[T, TF1, TF2]

  /**
   * Adds fields to the current stream that are computed from record values.
   *
   * @param f1     A [[FieldStatement]] that describes the first new field.
   * @param f2     A [[FieldStatement]] that describes the second new field.
   * @param f3     A [[FieldStatement]] that describes the third new field.
   * @param joiner A [[TypeJoiner]] that can produce the output stream type.
   * @tparam TF1 The type of the first new field.
   * @tparam TF2 The type of the second new field.
   * @tparam TF3 The type of the third new field.
   * @return An[[Stream]] containing the input records with the new fields appended.
   */
  def addFields[TF1, TF2, TF3](f1: FieldStatement[T, TF1],
                               f2: FieldStatement[T, TF2],
                               f3: FieldStatement[T, TF3])
                              (implicit joiner: TypeJoiner[T, (TF1, TF2, TF3)]): Stream[joiner.OutputType] = macro StreamMacros.addFields3[T, TF1, TF2, TF3]

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
  def fullJoin[TOther](other: Stream[TOther]): JoinedStream[T, TOther] = {
    new JoinedStream[T, TOther](this.expr, other.expr, JoinType.FullEnrichmentJoin)
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
  def leftJoin[TOther](other: Stream[TOther]): JoinedStream[T, TOther] = {
    new JoinedStream[T, TOther](this.expr, other.expr, JoinType.LeftEnrichmentJoin)
  }

  /**
   * Defines a "left outer join" relationship between this stream and a windowed stream.
   *
   * @param other The windowed stream being joined.
   * @tparam TOther The record type of the windowed stream.
   * @return A [[LeftJoinedWindowedStream]] representing the joined streams.
   */
  def leftJoin[TOther](other: WindowedStream[TOther]): LeftJoinedWindowedStream[T, TOther] = {
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
  def latestBy[TKey](dateExtractor: T => Instant, keyFunc: T => TKey): WindowedStream[T] = macro StreamMacros.latestBy[T, TKey]

  /**
   * Defines a grouping over records in the stream.
   *
   * @param keyFunc A function that computes the group key for a record.
   * @tparam TKey The type of group key.
   * @return A [[GroupedStream]] representing the result of the grouping operation.
   */
  def groupBy[TKey](keyFunc: T => TKey): GroupedStream[T, TKey] = macro StreamMacros.groupBy[T, TKey]

  /**
   * Defines a stream of tumbling windows over a date/time that is extracted from stream records.
   *
   * @param dateExtractor A function that extracts a date/time from a record.
   * @param windowPeriod  The length of a window.
   * @param offset        By default windows are aligned with the epoch, 1970-01-01.
   *                      This offset shifts the window alignment to the specified duration after the epoch.
   * @return A [[WindowedStream]] representing the result of the windowing operation.
   */
  def tumblingWindow(dateExtractor: T => Instant, windowPeriod: Duration, offset: Duration): WindowedStream[T] = macro StreamMacros.tumblingWindow[T]

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
  def slidingWindow(dateExtractor: T => Instant, windowSize: Duration, slide: Duration, offset: Duration): WindowedStream[T] = macro StreamMacros.slidingWindow[T]
}


object Stream {
  /**
   * Creates a [[Stream]]`[`[[T]]`]` representing a stream of objects of the specified type.
   *
   * @tparam T The type of objects.
   * @return A [[Stream]]`[`[[T]]`]` representing the stream.
   */
  def of[T]: Stream[T] = macro StreamMacros.of[T]

  /**
   * Creates a [[Stream]]`[`[[T]]`]` representing a stream of named tuples of the specified type.
   *
   * @param fieldNames The field names corresponding to the tuple type parameters.
   * @tparam T The type of tuple objects.
   * @return A [[Stream]]`[`[[T]]`]` representing the stream.
   */
  def ofFields[T <: Product](fieldNames: String*): Stream[T] = macro StreamMacros.ofFields[T]
}


object JoinType extends Enumeration {
  type JoinType = Value

  val LeftEnrichmentJoin, FullEnrichmentJoin = Value
}
