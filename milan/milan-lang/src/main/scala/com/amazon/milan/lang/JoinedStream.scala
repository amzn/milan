package com.amazon.milan.lang

import com.amazon.milan.Id
import com.amazon.milan.lang.JoinType.JoinType
import com.amazon.milan.lang.internal.JoinedStreamMacros
import com.amazon.milan.program.{FieldDefinition, Filter, FunctionDef, JoinExpression, MapFields, SelectField, SelectTerm, StreamExpression}
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
  /**
   * Defines the output of a join operation after the join conditions have been specified.
   * The output will be a stream containing the union of the fields of the input streams.
   *
   * @param joiner A [[TypeJoiner]] that can join the input record types.
   * @return A [[Stream]] representing the resulting output stream.
   */
  def selectAll()(implicit joiner: TypeJoiner[TLeft, TRight]): Stream[joiner.OutputType] = {
    val sourceExpression = this.expr
    val Filter(JoinExpression(left, right), _) = sourceExpression
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
  def select[TOut](f: (TLeft, TRight) => TOut): Stream[TOut] = macro JoinedStreamMacros.selectObject[TLeft, TRight, TOut]

  /**
   * Defines the output of a join operation after the join conditions have been specified.
   *
   * @param f A [[Function2FieldStatement]] that computes an output field.
   * @tparam TF The type of the output field.
   * @return A [[Stream]] representing the resulting output stream.
   */
  def select[TF](f: Function2FieldStatement[TLeft, TRight, TF]): Stream[Tuple1[TF]] = macro JoinedStreamMacros.selectTuple1[TLeft, TRight, TF]

  /**
   * Defines the output of a join operation after the join conditions have been specified.
   *
   * @param f1 A [[Function2FieldStatement]] that computes an output field.
   * @param f2 A [[Function2FieldStatement]] that computes an output field.
   * @tparam T1 The type of the first output field.
   * @tparam T2 The type of the second output field.
   * @return A [[Stream]] representing the resulting output stream.
   */
  def select[T1, T2](f1: Function2FieldStatement[TLeft, TRight, T1],
                     f2: Function2FieldStatement[TLeft, TRight, T2]): Stream[(T1, T2)] = macro JoinedStreamMacros.selectTuple2[TLeft, TRight, T1, T2]

  /**
   * Defines the output of a join operation after the join conditions have been specified.
   *
   * @return A [[Stream]] representing the resulting output stream.
   */
  def select[T1, T2, T3](f1: Function2FieldStatement[TLeft, TRight, T1],
                         f2: Function2FieldStatement[TLeft, TRight, T2],
                         f3: Function2FieldStatement[TLeft, TRight, T3]): Stream[(T1, T2, T3)] = macro JoinedStreamMacros.selectTuple3[TLeft, TRight, T1, T2, T3]

  /**
   * Defines the output of a join operation after the join conditions have been specified.
   *
   * @return A [[Stream]] representing the resulting output stream.
   */
  def select[T1, T2, T3, T4](f1: Function2FieldStatement[TLeft, TRight, T1],
                             f2: Function2FieldStatement[TLeft, TRight, T2],
                             f3: Function2FieldStatement[TLeft, TRight, T3],
                             f4: Function2FieldStatement[TLeft, TRight, T4]): Stream[(T1, T2, T3, T4)] = macro JoinedStreamMacros.selectTuple4[TLeft, TRight, T1, T2, T3, T4]

  /**
   * Defines the output of a join operation after the join conditions have been specified.
   *
   * @return A [[Stream]] representing the resulting output stream.
   */
  def select[T1, T2, T3, T4, T5](f1: Function2FieldStatement[TLeft, TRight, T1],
                                 f2: Function2FieldStatement[TLeft, TRight, T2],
                                 f3: Function2FieldStatement[TLeft, TRight, T3],
                                 f4: Function2FieldStatement[TLeft, TRight, T4],
                                 f5: Function2FieldStatement[TLeft, TRight, T5]): Stream[(T1, T2, T3, T4, T5)] = macro JoinedStreamMacros.selectTuple5[TLeft, TRight, T1, T2, T3, T4, T5]

  /**
   * Gets a [[MapFields]] expression that performs the mapping for a selectAll() operation.
   */
  private def getSelectAllMapExpression(source: StreamExpression,
                                        leftType: TypeDescriptor[_],
                                        rightType: TypeDescriptor[_],
                                        outputType: TypeDescriptor[_]): MapFields = {
    val leftFieldCount = if (leftType.isTuple) leftType.fields.length else 1
    val leftOutputFields = outputType.fields.take(leftFieldCount)
    val rightOutputFields = outputType.fields.drop(leftFieldCount)

    val leftFieldDefs =
      if (leftType.isTuple) {
        leftType.fields.zip(leftOutputFields)
          .map {
            case (inputField, outputField) => FieldDefinition(outputField.name, FunctionDef(List("l", "r"), SelectField(SelectTerm("l"), inputField.name)))
          }
      }
      else {
        val outputField = leftOutputFields.head
        List(FieldDefinition(outputField.name, FunctionDef(List("l", "r"), SelectTerm("l"))))
      }

    val rightFieldDefs =
      if (rightType.isTuple) {
        rightType.fields.zip(rightOutputFields)
          .map {
            case (inputField, outputField) => FieldDefinition(outputField.name, FunctionDef(List("l", "r"), SelectField(SelectTerm("r"), inputField.name)))
          }
      }
      else {
        val outputField = rightOutputFields.head
        List(FieldDefinition(outputField.name, FunctionDef(List("l", "r"), SelectTerm("r"))))
      }

    val outputFields = leftFieldDefs ++ rightFieldDefs
    val id = Id.newId()
    val streamType = new DataStreamTypeDescriptor(outputType)
    new MapFields(source, outputFields, id, id, streamType)
  }

}
