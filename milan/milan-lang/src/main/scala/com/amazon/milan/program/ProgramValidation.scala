package com.amazon.milan.program

object ProgramValidation {
  /**
   * Validates the Milan expression tree for a map function.
   *
   * @param function An expression tree representing a map function.
   */
  def validateMapFunction(function: FunctionDef, inputCount: Int): Unit = {
    if (function.arguments.length != inputCount) {
      throw new InvalidProgramException(s"Incorrect number of function arguments. Expected $inputCount, found ${function.arguments.length}.")
    }
  }

  /**
   * Validates that no duplicate argument names are used in a function.
   * Throws [[InvalidProgramException]] if any shadowing is detected.
   *
   * @param function A function definition.
   */
  def validateNoShadowedArguments(function: FunctionDef): Unit = {
    validateNoShadowedArguments(function.expr, function.arguments.toSet)
  }

  /**
   * Validates that no duplicate argument names are used in an expression tree.
   * Throws [[InvalidProgramException]] if any shadowing is detected.
   *
   * @param tree     An expression tree.
   * @param argNames The argument names in scope for the expression tree.
   */
  private def validateNoShadowedArguments(tree: Tree, argNames: Set[String]): Unit = {
    tree match {
      case Unpack(_, unpackArgNames, body) =>
        if (unpackArgNames.exists(argNames.contains)) {
          val duplicateName = unpackArgNames.toSet.union(argNames).head
          throw new InvalidProgramException(s"Shadowing of existing argument named '$duplicateName' is not allowed.")
        }

        // Combine the argument names, but ignore underscores since we don't care if we re-use it later.
        val combinedArgNames = (argNames ++ unpackArgNames) - "_"
        this.validateNoShadowedArguments(body, combinedArgNames)

      case _ =>
        tree.getChildren.foreach(child => this.validateNoShadowedArguments(child, argNames))
    }
  }
}
