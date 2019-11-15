package com.amazon.milan.program.internal

import com.amazon.milan.Id
import com.amazon.milan.program.{ComputedStream, Filter}

import scala.reflect.macros.whitebox

/**
 * Trait enabling macro bundles to create [[ComputedStream]] graph nodes that contain filter operations.
 */
trait FilteredStreamHost extends ConvertExpressionHost with ProgramTypeNamesHost {
  val c: whitebox.Context

  import c.universe._

  /**
   * Creates an expression that evaluates to a [[ComputedStream]] object, for a filter operation that yields a
   * record stream.
   *
   * @param predicate A filter predicate expression.
   * @tparam T The type of the stream being filtered.
   * @return An expression that evaluates to a [[ComputedStream]] object.
   */
  def createdFilteredStream[T: c.WeakTypeTag](predicate: c.Expr[T => Boolean]): c.Expr[ComputedStream] = {
    val outputNodeId = Id.newId()
    val predicateExpression = getMilanFunction(predicate.tree)
    val inputNodeVal = TermName(c.freshName())
    val exprNodeVal = TermName(c.freshName())

    val tree =
      q"""
          val $inputNodeVal = ${c.prefix}.node
          val $exprNodeVal = new ${typeOf[Filter]}($inputNodeVal.getStreamExpression, $predicateExpression, $outputNodeId, $outputNodeId, $inputNodeVal.getExpression.tpe)
          new ${typeOf[ComputedStream]}($outputNodeId, $outputNodeId, $exprNodeVal)
       """
    c.Expr[ComputedStream](tree)
  }
}
