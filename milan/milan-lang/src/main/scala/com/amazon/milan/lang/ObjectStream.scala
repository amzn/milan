package com.amazon.milan.lang

import com.amazon.milan.lang.internal.{ObjectStreamMacros, StreamMacros}
import com.amazon.milan.program.{ComputedStream, FieldDefinition, FunctionDef, MapFields, SelectTerm}
import com.amazon.milan.types.Record
import com.amazon.milan.typeutil.FieldDescriptor
import com.amazon.milan.{Id, program}

import scala.language.experimental.macros


/**
 * Represents a stream of objects of a given type.
 *
 * @param node The graph node containing information about this stream.
 * @tparam T The type of objects on the stream.
 */
class ObjectStream[T <: Record](node: program.Stream) extends Stream[T, ObjectStream[T]](node) {
  /**
   * Gets a copy of this [[ObjectStream]] object with the specified name assigned.
   *
   * @param name The name to assign.
   * @return A copy of the stream with the specified name assigned.
   */
  override def withName(name: String): ObjectStream[T] = {
    new ObjectStream[T](node.withName(name))
  }

  /**
   * Gets a copy of this [[Stream]] object with the specified ID assigned.
   *
   * @param id The ID to assign.
   * @return A copy of the stream with the specified ID assigned.
   */
  override def withId(id: String): ObjectStream[T] = {
    new ObjectStream[T](node.withId(id))
  }

  /**
   * Define a map relationship between this [[Stream]] and a [[TupleStream]] stream with one field.
   *
   * @param f A function that maps the objects on this stream to a named field.
   * @tparam TF The type of the field.
   * @return A [[TupleStream]] representing the output mapped stream.
   */
  def map[TF](f: FieldStatement[T, TF]): TupleStream[Tuple1[TF]] = macro StreamMacros.mapTuple1[T, TF]

  /**
   * Define a filter relationship between this [[ObjectStream]] and another [[ObjectStream]] of the same type.
   *
   * @param predicate A predicate that indicates which records from this stream are present on the output stream.
   * @return An [[ObjectStream]] representing the output filtered stream.
   */
  def where(predicate: T => Boolean): ObjectStream[T] = macro ObjectStreamMacros.where[T]

  /**
   * Converts this [[ObjectStream]] to a [[TupleStream]] with one field whose values contain the records from
   * this stream.
   *
   * @param fieldName The name of the field to use in the output stream.
   * @return A [[TupleStream]] with one field with the specified name.
   */
  def toField(fieldName: String): TupleStream[Tuple1[T]] = {
    // Create a MapFields expression that maps the input records into a single field.
    val fieldDef = FieldDefinition(fieldName, FunctionDef(List("r"), SelectTerm("r")))
    val mapExpr = MapFields(this.node.getExpression, List(fieldDef))
    val newId = Id.newId()
    val mapNode = ComputedStream(newId, newId, mapExpr)
    val fieldDescriptor = FieldDescriptor(fieldName, this.getRecordType)
    new TupleStream[Tuple1[T]](mapNode, List(fieldDescriptor))
  }
}
