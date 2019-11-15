package com.amazon.milan.lang.internal

import com.amazon.milan.Id
import com.amazon.milan.lang.{Function2FieldStatement, JoinedStream, JoinedStreamWithCondition, ObjectStream, TupleStream}
import com.amazon.milan.program.internal.ConvertExpressionHost
import com.amazon.milan.program.{ComputedGraphNode, FullJoin, LeftJoin}
import com.amazon.milan.types.Record

import scala.reflect.macros.whitebox


/**
 * Macro bundle for operations on [[JoinedStream]] objects.
 *
 * @param c The macro context.
 */
class JoinedStreamMacros(val c: whitebox.Context) extends StreamMacroHost with ConvertExpressionHost with FieldStatementHost with LangTypeNamesHost {

  import c.universe._

  /**
   * Creates a [[JoinedStreamWithCondition]] given a condition expression.
   *
   * @param conditionPredicate The condition expression.
   * @tparam TLeft  The type of the left stream.
   * @tparam TRight The type of the right stream.
   * @return An expression that evaluates to a [[JoinedStreamWithCondition]] object.
   */
  def where[TLeft: c.WeakTypeTag, TRight: c.WeakTypeTag](conditionPredicate: c.Expr[(TLeft, TRight) => Boolean]): c.Expr[JoinedStreamWithCondition[TLeft, TRight]] = {
    val leftTypeInfo = createTypeInfo[TLeft](c.weakTypeOf[TLeft])
    val rightTypeInfo = createTypeInfo[TRight](c.weakTypeOf[TRight])

    val conditionExpr = getMilanFunction(conditionPredicate.tree)

    val inputStreamVal = TermName(c.freshName())
    val streamExpr = TermName(c.freshName())
    val outNodeVal = TermName(c.freshName())
    val outNodeId = Id.newId()

    val tree =
      q"""
          com.amazon.milan.lang.internal.ProgramValidation.validateFunction($conditionExpr, 2)

          val $inputStreamVal = ${c.prefix}
          val $streamExpr = $inputStreamVal.joinType match {
            case com.amazon.milan.lang.JoinType.LeftEnrichmentJoin => new ${typeOf[LeftJoin]}($inputStreamVal.leftInput.getStreamExpression, $inputStreamVal.rightInput.getStreamExpression, $conditionExpr, $outNodeId, $outNodeId)
            case com.amazon.milan.lang.JoinType.FullEnrichmentJoin => new ${typeOf[FullJoin]}($inputStreamVal.leftInput.getStreamExpression, $inputStreamVal.rightInput.getStreamExpression, $conditionExpr, $outNodeId, $outNodeId)
          }
          val $outNodeVal = new ${typeOf[ComputedGraphNode]}($outNodeId, $streamExpr)
          new ${joinedStreamWithConditionTypeName(leftTypeInfo.ty, rightTypeInfo.ty)}($outNodeVal)
       """

    c.Expr[JoinedStreamWithCondition[TLeft, TRight]](tree)
  }

  /**
   * Creates an [[ObjectStream]] by mapping a joined stream via a map function.
   *
   * @param f The map function expression.
   * @tparam TLeft  The type of the left stream.
   * @tparam TRight The type of the right stream.
   * @tparam TOut   The output type of the map function.
   * @return An [[ObjectStream]] representing the result of the select operation.
   */
  def selectObject[TLeft: c.WeakTypeTag, TRight: c.WeakTypeTag, TOut <: Record : c.WeakTypeTag](f: c.Expr[(TLeft, TRight) => TOut]): c.Expr[ObjectStream[TOut]] = {
    val nodeTree = createMappedToRecordStream2[TLeft, TRight, TOut](f)
    val outputType = c.weakTypeOf[TOut]
    val tree = q"new ${objectStreamTypeName(outputType)}($nodeTree)"
    c.Expr[ObjectStream[TOut]](tree)
  }

  /**
   * Creates a [[TupleStream]] from a field statement.
   *
   * @param f The definition of the field.
   * @tparam TLeft  The type of the left input stream.
   * @tparam TRight The type of the right input stream.
   * @tparam TF     The output field type.
   * @return An expression that evaluates to a [[TupleStream]] of the output tuple type.
   */
  def selectTuple1[TLeft: c.WeakTypeTag, TRight: c.WeakTypeTag, TF: c.WeakTypeTag]
  (f: c.Expr[Function2FieldStatement[TLeft, TRight, TF]]): c.Expr[TupleStream[Tuple1[TF]]] = {
    val expr1 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, TF](f)
    mapTuple[Tuple1[TF]](List((expr1, c.weakTypeOf[TF])))
  }

  def selectTuple2[TLeft: c.WeakTypeTag, TRight: c.WeakTypeTag, T1: c.WeakTypeTag, T2: c.WeakTypeTag]
  (f1: c.Expr[Function2FieldStatement[TLeft, TRight, T1]],
   f2: c.Expr[Function2FieldStatement[TLeft, TRight, T2]]): c.Expr[TupleStream[(T1, T2)]] = {
    val expr1 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, T1](f1)
    val expr2 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, T2](f2)
    mapTuple[(T1, T2)](List((expr1, c.weakTypeOf[T1]), (expr2, c.weakTypeOf[T2])))
  }

  def selectTuple3[TLeft: c.WeakTypeTag, TRight: c.WeakTypeTag, T1: c.WeakTypeTag, T2: c.WeakTypeTag, T3: c.WeakTypeTag]
  (f1: c.Expr[Function2FieldStatement[TLeft, TRight, T1]],
   f2: c.Expr[Function2FieldStatement[TLeft, TRight, T2]],
   f3: c.Expr[Function2FieldStatement[TLeft, TRight, T3]]): c.Expr[TupleStream[(T1, T2, T3)]] = {
    val expr1 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, T1](f1)
    val expr2 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, T2](f2)
    val expr3 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, T3](f3)
    mapTuple[(T1, T2, T3)](
      List(
        (expr1, c.weakTypeOf[T1]),
        (expr2, c.weakTypeOf[T2]),
        (expr3, c.weakTypeOf[T3])))
  }

  def selectTuple4[TLeft: c.WeakTypeTag, TRight: c.WeakTypeTag, T1: c.WeakTypeTag, T2: c.WeakTypeTag, T3: c.WeakTypeTag, T4: c.WeakTypeTag]
  (f1: c.Expr[Function2FieldStatement[TLeft, TRight, T1]],
   f2: c.Expr[Function2FieldStatement[TLeft, TRight, T2]],
   f3: c.Expr[Function2FieldStatement[TLeft, TRight, T3]],
   f4: c.Expr[Function2FieldStatement[TLeft, TRight, T4]]): c.Expr[TupleStream[(T1, T2, T3, T4)]] = {
    val expr1 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, T1](f1)
    val expr2 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, T2](f2)
    val expr3 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, T3](f3)
    val expr4 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, T4](f4)
    mapTuple[(T1, T2, T3, T4)](
      List(
        (expr1, c.weakTypeOf[T1]),
        (expr2, c.weakTypeOf[T2]),
        (expr3, c.weakTypeOf[T3]),
        (expr4, c.weakTypeOf[T4])))
  }

  def selectTuple5[TLeft: c.WeakTypeTag, TRight: c.WeakTypeTag, T1: c.WeakTypeTag, T2: c.WeakTypeTag, T3: c.WeakTypeTag, T4: c.WeakTypeTag, T5: c.WeakTypeTag]
  (f1: c.Expr[Function2FieldStatement[TLeft, TRight, T1]],
   f2: c.Expr[Function2FieldStatement[TLeft, TRight, T2]],
   f3: c.Expr[Function2FieldStatement[TLeft, TRight, T3]],
   f4: c.Expr[Function2FieldStatement[TLeft, TRight, T4]],
   f5: c.Expr[Function2FieldStatement[TLeft, TRight, T5]]): c.Expr[TupleStream[(T1, T2, T3, T4, T5)]] = {
    val expr1 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, T1](f1)
    val expr2 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, T2](f2)
    val expr3 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, T3](f3)
    val expr4 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, T4](f4)
    val expr5 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, T5](f5)
    mapTuple[(T1, T2, T3, T4, T5)](
      List(
        (expr1, c.weakTypeOf[T1]),
        (expr2, c.weakTypeOf[T2]),
        (expr3, c.weakTypeOf[T3]),
        (expr4, c.weakTypeOf[T4]),
        (expr5, c.weakTypeOf[T5])))
  }
}
