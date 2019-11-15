package com.amazon.milan.lang.internal

import com.amazon.milan.lang.{FieldStatement, Function2FieldStatement}
import com.amazon.milan.program.FieldDefinition
import com.amazon.milan.program.internal.ConvertExpressionHost

import scala.reflect.api.Trees
import scala.reflect.macros.whitebox


/**
 * Trait enabling macro bundles to handle of [[FieldStatement]] and [[Function2FieldStatement]] expressions.
 */
trait FieldStatementHost extends ConvertExpressionHost {
  val c: whitebox.Context

  import c.universe._

  /**
   * Gets [[FieldDefinition]] object representing the specified field calculation statement.
   *
   * @param fieldStatement An expression that produces a [[FieldStatement]] object.
   * @return A [[FieldDefinition]] object.
   */
  def getFieldDefinition[TIn, TF: c.WeakTypeTag](fieldStatement: c.Expr[FieldStatement[TIn, TF]]): c.Expr[FieldDefinition] = {
    // If the fields are specified in the proscribed way, i.e.
    // ((x: TIn) => object.method(x)) as "name"
    // then the expression tree can be extracted as below.
    // Any other form and the extraction will fail.
    val q"$fieldFunctionCreationStatement[$inputType, $outputType]($statementBody).as($fieldNameExpr)" = fieldStatement.tree
    this.getFieldDefinition(fieldNameExpr, statementBody, 1)
  }


  /**
   * Gets a [[FieldDefinition]] object representing the specified field calculation statement for a joined
   * stream select statement.
   *
   * @param fieldStatement An expression that produces a [[Function2FieldStatement]] object.
   * @return A [[FieldDefinition]] object.
   */
  def getFieldDefinitionForSelectFromJoin[TLeft, TRight, TF: c.WeakTypeTag](fieldStatement: c.Expr[Function2FieldStatement[TLeft, TRight, TF]]): c.Expr[FieldDefinition] = {
    // If the fields are specified in the proscribed way, i.e.
    // ((l: TLeft, r: TRight) => object.method(l, r)) as "name"
    // then the expression tree can be extracted as below.
    // Any other form and the extraction will fail.
    val q"$fieldFunctionCreationStatement[$leftInputType, $rightInputType, $outputType]($statementBody).as($fieldNameExpr)" = fieldStatement.tree
    this.getFieldDefinition(fieldNameExpr, statementBody, 2)
  }

  /**
   * Gets [[FieldDefinition]] object representing the specified field calculation statement for a grouped
   * stream select statement.
   *
   * @param streamType     [[TypeInfo]] for records on the stream.
   * @param groupKeyType   [[TypeInfo]] for the group key type.
   * @param fieldStatement An expression that produces a [[Function2FieldStatement]] object.
   * @return A [[FieldDefinition]] object.
   */
  def getFieldDefinitionForSelectFromGroupBy[TIn, TKey, TF: c.WeakTypeTag](streamType: TypeInfo[TIn],
                                                                           groupKeyType: TypeInfo[TKey],
                                                                           fieldStatement: c.Expr[Function2FieldStatement[TKey, TIn, TF]]): c.Expr[FieldDefinition] = {
    val q"$fieldFunctionCreationStatement[$keyType, $inputType, $outputType]($statementBody).as($fieldNameExpr)" = fieldStatement.tree
    val field = this.getFieldDefinition(fieldNameExpr, statementBody, 2)

    val tree =
      q"""
          com.amazon.milan.lang.internal.ProgramValidation.validateSelectFromGroupByFunction($field.expr)
          $field
       """
    c.Expr[FieldDefinition](tree)
  }

  private def getFieldDefinition(fieldNameExpression: Trees#Tree,
                                 statementBody: Trees#Tree,
                                 inputCount: Int): c.Expr[FieldDefinition] = {
    val fieldName = this.getFieldName(fieldNameExpression)
    val fieldFunction = getMilanFunction(statementBody.asInstanceOf[c.universe.Tree])

    val tree =
      q"""
          com.amazon.milan.lang.internal.ProgramValidation.validateFunction($fieldFunction, $inputCount)
          new ${typeOf[FieldDefinition]}($fieldName, $fieldFunction)
       """

    c.Expr[FieldDefinition](tree)
  }

  private def getFieldName(fieldNameExpr: Trees#Tree): String = {
    // fieldNameExpr will be a string literal expression, but we can't just call toString on it otherwise we'll
    // get the enclosing quotes as part of the name.
    fieldNameExpr match {
      case Literal(Constant(value)) =>
        value.asInstanceOf[String]

      case _ =>
        c.abort(c.enclosingPosition, s"Field names must be compile-time constants.")
    }
  }
}
