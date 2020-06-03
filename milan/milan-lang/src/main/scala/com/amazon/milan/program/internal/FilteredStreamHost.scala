package com.amazon.milan.program.internal

import com.amazon.milan.Id
import com.amazon.milan.program.Filter

import scala.reflect.macros.whitebox

/**
 * Trait enabling macro bundles to create [[Filter]] expressions.
 */
trait FilteredStreamHost extends ConvertExpressionHost with ProgramTypeNamesHost {
  val c: whitebox.Context

  import c.universe._

  /**
   * Creates an expression that evaluates to a [[Filter]] object, for a filter operation that yields a
   * record stream.
   *
   * @param predicate A filter predicate expression.
   * @tparam T The type of the stream being filtered.
   * @return An expression that evaluates to a [[Filter]] object.
   */
  def createdFilteredStream[T: c.WeakTypeTag](predicate: c.Expr[T => Boolean]): c.Expr[Filter] = {
    val outputNodeId = Id.newId()
    val predicateExpression = getMilanFunction(predicate.tree)
    val inputExprVal = TermName(c.freshName("inputExpr"))

    val tree =
      q"""
          val $inputExprVal = ${c.prefix}.expr
          new ${typeOf[Filter]}($inputExprVal, $predicateExpression, $outputNodeId, $outputNodeId, $inputExprVal.tpe)
       """
    c.Expr[Filter](tree)
  }
}
