package com.amazon.milan.program.internal

import com.amazon.milan.Id
import com.amazon.milan.program._

import scala.reflect.macros.whitebox

/**
 * Trait enabling macro bundles to create [[StreamMap]] expressions.
 */
trait MappedStreamHost extends ConvertExpressionHost with ProgramTypeNamesHost with RecordTypeHost {
  val c: whitebox.Context

  import c.universe._

  /**
   * Creates an expression that evaluates to a [[StreamMap]] object, for a map operation that yields a record stream.
   *
   * @param f A map function expression.
   * @tparam TIn  The input type of the map function.
   * @tparam TOut The output type of the map function.
   * @return An expression that evaluates to a [[StreamMap]] object.
   */
  def createMappedToRecordStream[TIn: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[TIn => TOut]): c.Expr[StreamMap] = {
    val mapExpression = getMilanFunction(f.tree)
    val validationExpression = q"com.amazon.milan.program.ProgramValidation.validateMapFunction($mapExpression, 1)"
    this.createStreamMap[TOut](mapExpression, validationExpression)
  }

  /**
   * Creates an expression that evaluates to a [[StreamExpression]] object, for a map operation with two input streams that
   * yields a record stream.
   *
   * @param f A map function expression.
   * @tparam T1   The type of the first argument of the map function.
   * @tparam T2   The type of the second argument of the map function.
   * @tparam TOut The output type of the map function.
   * @return An expression that evaluates to a [[StreamExpression]] object.
   */
  def createMappedToRecordStream2[T1: c.WeakTypeTag, T2: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[(T1, T2) => TOut]): c.Expr[StreamExpression] = {
    val mapExpression = getMilanFunction(f.tree)
    val validationExpression = q"com.amazon.milan.program.ProgramValidation.validateMapFunction($mapExpression, 2)"
    this.createStreamMap[TOut](mapExpression, validationExpression)
  }

  /**
   * Creates an expression that evaluates to a [[StreamMap]] object, for a map operation that yields a record stream.
   *
   * @param mapFunction The [[FunctionDef]] containing the map expression.
   * @tparam TOut The output type of the map function.
   * @return An expression that evaluates to a [[StreamMap]] object.
   */
  def createStreamMap[TOut: c.WeakTypeTag](mapFunction: c.Expr[FunctionDef],
                                           validationExpression: c.universe.Tree): c.Expr[StreamMap] = {
    val streamType = getStreamTypeExpr[TOut](mapFunction)
    val outputNodeId = Id.newId()

    val sourceExpressionVal = TermName(c.freshName("sourceExpr"))

    val tree =
      q"""
          $validationExpression

          val $sourceExpressionVal = ${c.prefix}.expr
          new ${typeOf[StreamMap]}($sourceExpressionVal, $mapFunction, $outputNodeId, $outputNodeId, $streamType)
       """
    c.Expr[StreamMap](tree)
  }

  def createAggregate[TOut: c.WeakTypeTag](aggregateFunction: c.Expr[FunctionDef],
                                           validationExpression: c.universe.Tree): c.Expr[Aggregate] = {
    val streamType = getStreamTypeExpr[TOut](aggregateFunction)
    val outputNodeId = Id.newId()

    val sourceExpressionVal = TermName(c.freshName("sourceExpr"))

    val tree =
      q"""
          $validationExpression

          val $sourceExpressionVal = ${c.prefix}.expr
          new ${typeOf[Aggregate]}($sourceExpressionVal, $aggregateFunction, $outputNodeId, $outputNodeId, $streamType)
       """
    c.Expr[Aggregate](tree)
  }
}
