package com.amazon.milan.lang

import com.amazon.milan.lang.internal.{StreamMacros, TupleStreamMacros}
import com.amazon.milan.program
import com.amazon.milan.types.Record
import com.amazon.milan.typeutil.{FieldDescriptor, StreamTypeDescriptor, TupleTypeDescriptor, TypeDescriptor, TypeJoiner}

import scala.language.experimental.macros


/**
 * Represents a stream of named fields.
 *
 * @param node The graph node containing information about this stream.
 * @tparam T The type of records on the stream.
 */
class TupleStream[T <: Product](node: program.Stream, val fields: List[FieldDescriptor[_]]) extends Stream[T, TupleStream[T]](node) {
  /**
   * Initializes a new instance of the [[TupleStream]] class using only a graph node.
   *
   * @param node The graph node representing the stream. The node's expression must have it's tpe property set to an
   *             instance of [[StreamTypeDescriptor]].
   */
  def this(node: program.Stream) {
    this(node, node.getExpression.tpe.asStream.recordType.fields)
  }

  /**
   * Gets a copy of this [[TupleStream]] object with the specified name assigned.
   *
   * @param name The name to assign.
   * @return A copy of the stream with the specified name assigned.
   */
  override def withName(name: String): TupleStream[T] = {
    new TupleStream[T](this.node.withName(name), this.fields)
  }

  /**
   * Gets a copy of this [[Stream]] object with the specified ID assigned.
   *
   * @param id The ID to assign.
   * @return A copy of the stream with the specified ID assigned.
   */
  override def withId(id: String): TupleStream[T] = {
    new TupleStream[T](this.node.withId(id), this.fields)
  }

  /**
   * Gets a [[TypeDescriptor]] describing the type of records on the stream.
   *
   * @return A [[TypeDescriptor]] for the stream records, or null if the type cannot be determined.
   */
  override def getRecordType: TypeDescriptor[T] = {
    new TupleTypeDescriptor[T](this.fields)
  }

  /**
   * Maps the stream to another stream with a single named field.
   *
   * @param f A field statement.
   * @tparam TF The type of the field.
   * @return A [[TupleStream]] representing the result of the map operation.
   * @note The can't be an overload of the map function because for some reason scala can't distinguish it from the
   *       "map to record" variant above.
   */
  def mapToField[TF](f: FieldStatement[T, TF]): TupleStream[Tuple1[TF]] = macro StreamMacros.mapTuple1[T, TF]

  /**
   * Applies a filter predicate to a [[TupleStream]].
   *
   * @param predicate A filter predicate function.
   * @return A [[TupleStream]] containing only the records where the predicate returns true.
   */
  def where(predicate: T => Boolean): TupleStream[T] = macro TupleStreamMacros.where[T]

  /**
   * Projects the fields from this stream onto the fields of the specified object record type.
   *
   * @tparam TTarget The type of record objects to project the stream fields onto.
   * @return An [[ObjectStream]] of the target record type.
   */
  def projectOnto[TTarget <: Record]: ObjectStream[TTarget] = macro TupleStreamMacros.projectOnto[T, TTarget]

  def addField[TF](f: FieldStatement[T, TF])(implicit joiner: TypeJoiner[T, Tuple1[TF]]): TupleStream[joiner.OutputType] = macro TupleStreamMacros.addField[T, TF]

  def addFields[TF1, TF2](f1: FieldStatement[T, TF1],
                          f2: FieldStatement[T, TF2])
                         (implicit joiner: TypeJoiner[T, (TF1, TF2)]): TupleStream[joiner.OutputType] = macro TupleStreamMacros.addFields2[T, TF1, TF2]

  def addFields[TF1, TF2, TF3](f1: FieldStatement[T, TF1],
                               f2: FieldStatement[T, TF2],
                               f3: FieldStatement[T, TF3])
                              (implicit joiner: TypeJoiner[T, (TF1, TF2, TF3)]): TupleStream[joiner.OutputType] = macro TupleStreamMacros.addFields3[T, TF1, TF2, TF3]
}
