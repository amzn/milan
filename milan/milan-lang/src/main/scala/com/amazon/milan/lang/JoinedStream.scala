package com.amazon.milan.lang

import com.amazon.milan.Id
import com.amazon.milan.lang.JoinType.JoinType
import com.amazon.milan.lang.internal.JoinedStreamMacros
import com.amazon.milan.program.{FunctionDef, JoinExpression, NamedField, NamedFields, SelectField, SelectTerm, StreamExpression, StreamMap, ValueDef}
import com.amazon.milan.typeutil.{DataStreamTypeDescriptor, TypeDescriptor, TypeJoiner}

import scala.language.existentials
import scala.language.experimental.macros


/**
 * Represents the result of a join operation between two fields.
 *
 * @param leftInput  The left input stream.
 * @param rightInput The right input stream.
 * @tparam TLeft  The type of the left stream.
 * @tparam TRight The type of the right stream.
 */
class JoinedStream[TLeft, TRight](val leftInput: StreamExpression, val rightInput: StreamExpression, val joinType: JoinType) {
  /**
   * Applies conditions to a join operation.
   *
   * @param conditionPredicate A boolean function that defines the join conditions.
   * @return A [[JoinedStreamWithCondition]] object representing the resulting conditioned join.
   */
  def where(conditionPredicate: (TLeft, TRight) => Boolean): JoinedStreamWithCondition[TLeft, TRight] = macro JoinedStreamMacros.where[TLeft, TRight]
}


class JoinedStreamWithCondition[TLeft, TRight](val expr: StreamExpression) {
  val leftState = new StateIdentifier("left")
  val rightState = new StateIdentifier("right")

  /**
   * Defines the output of a join operation after the join conditions have been specified.
   * The output will be a stream containing the union of the fields of the input streams.
   *
   * @param joiner A [[TypeJoiner]] that can join the input record types.
   * @return A [[Stream]] representing the resulting output stream.
   */
  def selectAll()(implicit joiner: TypeJoiner[TLeft, TRight]): Stream[joiner.OutputType] = {
    val sourceExpression = this.expr
    val JoinExpression(left, right, _) = sourceExpression
    val leftRecordType = left.tpe.asStream.recordType.asInstanceOf[TypeDescriptor[TLeft]]
    val rightRecordType = right.tpe.asStream.recordType.asInstanceOf[TypeDescriptor[TRight]]
    val joinedType = joiner.getOutputType(leftRecordType, rightRecordType)

    val mapExpr = this.getSelectAllMapExpression(sourceExpression, leftRecordType, rightRecordType, joinedType)

    new Stream[joiner.OutputType](mapExpr, joinedType)
  }

  /**
   * Defines the output of a join operation after the join conditions have been specified.
   *
   * @param f A function that computes the output.
   * @tparam TOut The type of the output.
   * @return A [[Stream]] representing the resulting output stream.
   */
  def select[TOut](f: (TLeft, TRight) => TOut): Stream[TOut] = macro JoinedStreamMacros.select[TLeft, TRight, TOut]

  /**
   * Gets a [[StreamMap]] expression that performs the mapping for a selectAll() operation.
   */
  private def getSelectAllMapExpression(source: StreamExpression,
                                        leftType: TypeDescriptor[_],
                                        rightType: TypeDescriptor[_],
                                        outputType: TypeDescriptor[_]): StreamMap = {
    val leftFieldCount = if (leftType.isTuple) leftType.fields.length else 1
    val leftOutputFields = outputType.fields.take(leftFieldCount)
    val rightOutputFields = outputType.fields.drop(leftFieldCount)

    val leftFieldDefs =
      if (leftType.isTuple) {
        leftType.fields.zip(leftOutputFields)
          .map {
            case (inputField, outputField) => NamedField(outputField.name, SelectField(SelectTerm("l"), inputField.name))
          }
      }
      else {
        val outputField = leftOutputFields.head
        List(NamedField(outputField.name, SelectTerm("l")))
      }

    val rightFieldDefs =
      if (rightType.isTuple) {
        rightType.fields.zip(rightOutputFields)
          .map {
            case (inputField, outputField) => NamedField(outputField.name, SelectField(SelectTerm("r"), inputField.name))
          }
      }
      else {
        val outputField = rightOutputFields.head
        List(NamedField(outputField.name, SelectTerm("r")))
      }

    val outputFields = leftFieldDefs ++ rightFieldDefs
    val mapFunction = FunctionDef(List(ValueDef("l", leftType), ValueDef("r", rightType)), NamedFields(outputFields))

    val id = Id.newId()
    val streamType = new DataStreamTypeDescriptor(outputType)
    new StreamMap(source, mapFunction, id, id, streamType)
  }
}
