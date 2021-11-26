package com.amazon.milan.lang.internal

import com.amazon.milan.program._


object ProgramValidation {
  /**
   * Validates a function used in a select() operation following a group-by or windowing operation.
   */
  def validateSelectFromGroupByFunction(function: FunctionDef): Unit = {
    validateFunction(function, 2)

    def isKeyArg(argName: String): Boolean = argName == function.arguments.head.name

    def validateInsideAggregateFunction(tree: Tree): Unit = {
      tree match {
        case _: UnaryAggregateExpression =>
          throw new InvalidProgramException("Aggregate function calls cannot be nested.")

        case SelectTerm(argName) if isKeyArg(argName) =>
          throw new InvalidProgramException("Group key cannot be used inside an aggregate function call.")

        case _ =>
          tree.getChildren.foreach(validateInsideAggregateFunction)
      }
    }

    def validateOutsideAggregateFunction(tree: Tree): Unit = {
      tree match {
        case a: UnaryAggregateExpression =>
          validateInsideAggregateFunction(a.expr)

        case _ =>
          tree.getChildren.foreach(validateOutsideAggregateFunction)
      }
    }

    validateOutsideAggregateFunction(function.body)
  }

  /**
   * Validates a function definition.
   */
  def validateFunction(function: FunctionDef, inputCount: Int): Unit = {
    if (function.arguments.length != inputCount) {
      throw new InvalidProgramException(s"Incorrect number of function arguments. Expected $inputCount, found ${function.arguments.length}.")
    }

    try {
      com.amazon.milan.program.ProgramValidation.validateNoShadowedArguments(function)
    }
    catch {
      case ex: InvalidProgramException =>
        throw new InvalidProgramException(ex.getMessage + s" Full function  definition: '$function'.", ex)
    }
  }
}
