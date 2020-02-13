package com.amazon.milan.lang.internal

import com.amazon.milan.Id
import com.amazon.milan.lang.{Function2FieldStatement, JoinType, JoinedStream, JoinedStreamWithCondition, Stream}
import com.amazon.milan.program.internal.ConvertExpressionHost
import com.amazon.milan.program.{Filter, FullJoin, FunctionDef, LeftJoin}
import com.amazon.milan.typeutil.{JoinedStreamsTypeDescriptor, TypeDescriptor}

import scala.reflect.macros.whitebox


/**
 * Macro bundle for operations on [[JoinedStream]] objects.
 *
 * @param c The macro context.
 */
class JoinedStreamMacros(val c: whitebox.Context) extends StreamMacroHost with ConvertExpressionHost with FieldStatementHost {

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
    val conditionExpr = getMilanFunction(conditionPredicate.tree)
    val inputStreamVal = TermName(c.freshName())

    val tree =
      q"""
          val $inputStreamVal = ${c.prefix}
          _root_.com.amazon.milan.lang.internal.JoinedStreamUtil.where($inputStreamVal, $conditionExpr)
        """

    c.Expr[JoinedStreamWithCondition[TLeft, TRight]](tree)
  }

  /**
   * Creates a [[Stream]] by mapping a joined stream via a map function.
   *
   * @param f The map function expression.
   * @tparam TLeft  The type of the left stream.
   * @tparam TRight The type of the right stream.
   * @tparam TOut   The output type of the map function.
   * @return A [[Stream]] representing the result of the select operation.
   */
  def selectObject[TLeft: c.WeakTypeTag, TRight: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[(TLeft, TRight) => TOut]): c.Expr[Stream[TOut]] = {
    this.warnIfNoRecordId[TOut]()

    val nodeTree = createMappedToRecordStream2[TLeft, TRight, TOut](f)
    val outputType = getTypeDescriptor[TOut]
    val tree = q"new ${weakTypeOf[Stream[TOut]]}($nodeTree, $outputType)"
    c.Expr[Stream[TOut]](tree)
  }

  /**
   * Creates a [[Stream]] from a field statement.
   *
   * @param f The definition of the field.
   * @tparam TLeft  The type of the left input stream.
   * @tparam TRight The type of the right input stream.
   * @tparam TF     The output field type.
   * @return An expression that evaluates to a [[Stream]] of the output tuple type.
   */
  def selectTuple1[TLeft: c.WeakTypeTag, TRight: c.WeakTypeTag, TF: c.WeakTypeTag]
  (f: c.Expr[Function2FieldStatement[TLeft, TRight, TF]]): c.Expr[Stream[Tuple1[TF]]] = {
    val expr1 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, TF](f)
    mapTuple[Tuple1[TF]](List((expr1, c.weakTypeOf[TF])))
  }

  def selectTuple2[TLeft: c.WeakTypeTag, TRight: c.WeakTypeTag, T1: c.WeakTypeTag, T2: c.WeakTypeTag]
  (f1: c.Expr[Function2FieldStatement[TLeft, TRight, T1]],
   f2: c.Expr[Function2FieldStatement[TLeft, TRight, T2]]): c.Expr[Stream[(T1, T2)]] = {
    val expr1 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, T1](f1)
    val expr2 = getFieldDefinitionForSelectFromJoin[TLeft, TRight, T2](f2)
    mapTuple[(T1, T2)](List((expr1, c.weakTypeOf[T1]), (expr2, c.weakTypeOf[T2])))
  }

  def selectTuple3[TLeft: c.WeakTypeTag, TRight: c.WeakTypeTag, T1: c.WeakTypeTag, T2: c.WeakTypeTag, T3: c.WeakTypeTag]
  (f1: c.Expr[Function2FieldStatement[TLeft, TRight, T1]],
   f2: c.Expr[Function2FieldStatement[TLeft, TRight, T2]],
   f3: c.Expr[Function2FieldStatement[TLeft, TRight, T3]]): c.Expr[Stream[(T1, T2, T3)]] = {
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
   f4: c.Expr[Function2FieldStatement[TLeft, TRight, T4]]): c.Expr[Stream[(T1, T2, T3, T4)]] = {
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
   f5: c.Expr[Function2FieldStatement[TLeft, TRight, T5]]): c.Expr[Stream[(T1, T2, T3, T4, T5)]] = {
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


object JoinedStreamUtil {
  def where[TLeft, TRight](inputStream: JoinedStream[TLeft, TRight], conditionExpr: FunctionDef): JoinedStreamWithCondition[TLeft, TRight] = {
    ProgramValidation.validateFunction(conditionExpr, 2)

    val joinNodeId = Id.newId()
    val joinedStreamType = new JoinedStreamsTypeDescriptor(inputStream.leftInput.recordType, inputStream.rightInput.recordType)

    val joinExpr = inputStream.joinType match {
      case JoinType.LeftEnrichmentJoin => new LeftJoin(inputStream.leftInput, inputStream.rightInput, joinNodeId, joinNodeId, joinedStreamType)
      case JoinType.FullEnrichmentJoin => new FullJoin(inputStream.leftInput, inputStream.rightInput, joinNodeId, joinNodeId, joinedStreamType)
    }

    val outNodeId = Id.newId()
    val streamExpr = new Filter(joinExpr, conditionExpr, outNodeId, outNodeId, joinedStreamType)

    new JoinedStreamWithCondition[TLeft, TRight](streamExpr)
  }
}
