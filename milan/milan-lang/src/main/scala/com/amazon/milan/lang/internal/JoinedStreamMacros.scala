package com.amazon.milan.lang.internal

import com.amazon.milan.Id
import com.amazon.milan.lang.{JoinType, JoinedStream, JoinedStreamWithCondition, Stream}
import com.amazon.milan.program.internal.ConvertExpressionHost
import com.amazon.milan.program.{FullJoin, FunctionDef, LeftInnerJoin, LeftJoin}
import com.amazon.milan.typeutil.{JoinedStreamsTypeDescriptor, TypeDescriptor}

import scala.reflect.macros.whitebox


/**
 * Macro bundle for operations on [[JoinedStream]] objects.
 *
 * @param c The macro context.
 */
class JoinedStreamMacros(val c: whitebox.Context) extends StreamMacroHost with ConvertExpressionHost {

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
    val inputStreamVal = TermName(c.freshName("inputStream"))

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
  def select[TLeft: c.WeakTypeTag, TRight: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[(TLeft, TRight) => TOut]): c.Expr[Stream[TOut]] = {
    this.warnIfNoRecordId[TOut]()

    val exprTree = createMappedToRecordStream2[TLeft, TRight, TOut](f)
    val exprVal = TermName(c.freshName("expr"))
    val tree =
      q"""
          val $exprVal = $exprTree
          new ${weakTypeOf[Stream[TOut]]}($exprVal, $exprVal.recordType.asInstanceOf[${weakTypeOf[TypeDescriptor[TOut]]}])
       """
    c.Expr[Stream[TOut]](tree)
  }
}


object JoinedStreamUtil {
  def where[TLeft, TRight](inputStream: JoinedStream[TLeft, TRight], conditionExpr: FunctionDef): JoinedStreamWithCondition[TLeft, TRight] = {
    ProgramValidation.validateFunction(conditionExpr, 2)

    val joinNodeId = Id.newId()
    val joinedStreamType = new JoinedStreamsTypeDescriptor(inputStream.leftInput.recordType, inputStream.rightInput.recordType)

    val joinExpr = inputStream.joinType match {
      case JoinType.LeftEnrichmentJoin => new LeftJoin(inputStream.leftInput, inputStream.rightInput, conditionExpr, joinNodeId, joinNodeId, joinedStreamType)
      case JoinType.FullEnrichmentJoin => new FullJoin(inputStream.leftInput, inputStream.rightInput, conditionExpr, joinNodeId, joinNodeId, joinedStreamType)
      case JoinType.LeftInnerJoin => new LeftInnerJoin(inputStream.leftInput, inputStream.rightInput, conditionExpr, joinNodeId, joinNodeId, joinedStreamType)
    }

    new JoinedStreamWithCondition[TLeft, TRight](joinExpr)
  }
}
