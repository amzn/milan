package com.amazon.milan.compiler.scala.trees

import com.amazon.milan.program.{And, Equals, FunctionDef, Tree, Tuple, TypeChecker, ValueDef}


object KeySelectorExtractor {
  /**
   * Gets functions that generate join key values from input streams.
   * These functions take one of the two input streams for the join, and produce a tuple containing the different parts
   * of the join key as determined by the join expression.
   *
   * @param joinExpressionFunction The definition of the join expression, as a function of the two input streams.
   * @return A tuple of functions that generate the join keys from the left and right input streams respectively.
   */
  def getKeyTupleFunctions(joinExpressionFunction: FunctionDef): (FunctionDef, FunctionDef) = {
    val (leftElements, rightElements) = this.getKeyElementExpressions(joinExpressionFunction)

    val leftArg = joinExpressionFunction.arguments.head
    val leftTupleFunction = FunctionDef(List(leftArg), Tuple(leftElements))

    val rightArg = joinExpressionFunction.arguments.last
    val rightTupleFunction = FunctionDef(List(rightArg), Tuple(rightElements))

    TypeChecker.typeCheck(leftTupleFunction)
    TypeChecker.typeCheck(rightTupleFunction)

    (leftTupleFunction, rightTupleFunction)
  }

  /**
   * Gets the expressions that are used to extract elements of join keys for each stream.
   *
   * @param keyFunction A [[FunctionDef]] containing the join condition expression.
   *                    This should be the result of using [[JoinKeyExpressionExtractor.extractJoinKeyExpression]].
   * @return A tuple of two lists of expressions. The first list are the key element expressions for the left stream,
   *         the second list are for the right stream. The two lists will have the same number of elements.
   */
  private def getKeyElementExpressions(keyFunction: FunctionDef): (List[Tree], List[Tree]) = {
    // Because the key function is the result JoinKeyExpressionExtractor.extractJoinKeyExpression, we know that it's a
    // conjunction of equality conditions, and that each side of every equality condition references exactly one of the
    // two input streams.
    keyFunction match {
      case FunctionDef(List(ValueDef(leftArgName, _), ValueDef(rightArgName, _)), body) =>
        this.getKeyElementExpressions(leftArgName, rightArgName, body)

      case _ =>
        throw new IllegalArgumentException(s"Unsupported key expression: $keyFunction")
    }
  }

  /**
   * Gets the expressions that are used to extract elements of join keys for each stream.
   *
   * @param leftArgName  The name of the argument corresponding to the left input stream.
   * @param rightArgName The name of the argument corresponding to the right input stream.
   * @param expr         The join condition expression.
   * @return A tuple of two lists of expressions. The first list are the key element expressions for the left stream,
   *         the second list are for the right stream. The two lists will have the same number of elements.
   */
  private def getKeyElementExpressions(leftArgName: String,
                                       rightArgName: String,
                                       expr: Tree): (List[Tree], List[Tree]) = {
    expr match {
      case And(left, right) =>
        val (leftExprs1, rightExprs1) = this.getKeyElementExpressions(leftArgName, rightArgName, left)
        val (leftExprs2, rightExprs2) = this.getKeyElementExpressions(leftArgName, rightArgName, right)
        (leftExprs1 ++ leftExprs2, rightExprs1 ++ rightExprs2)

      case Equals(left, right) =>
        val leftTreeArgName = this.getReferencedArgument(left)
        val rightTreeArgName = this.getReferencedArgument(right)
        if (leftTreeArgName == leftArgName && rightTreeArgName == rightArgName) {
          (List(left), List(right))
        }
        else if (leftTreeArgName == rightArgName && rightTreeArgName == leftArgName) {
          (List(right), List(left))
        }
        else {
          throw new IllegalArgumentException(s"Key expression references arguments '$leftTreeArgName' and '$rightTreeArgName', which do not match expected arguments '$leftArgName' and '$rightArgName'.")
        }

      case _ =>
        throw new IllegalArgumentException(s"Unsupported key expression: $expr")
    }
  }

  /**
   * Gets the name of the argument referenced in an expression tree.
   * If the tree references more or fewer than one argument, an exception is thrown.
   *
   * @param expr An expression tree that references exactly one argument.
   * @return The name of the referenced argument.
   */
  private def getReferencedArgument(expr: Tree): String = {
    val termNames = expr.listTermNames().toSet

    if (termNames.size == 1) {
      termNames.head
    }
    else {
      throw new IllegalArgumentException(s"More than one argument is referenced in join key element expression: $expr")
    }
  }
}
