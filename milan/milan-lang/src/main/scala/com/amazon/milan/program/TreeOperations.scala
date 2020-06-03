package com.amazon.milan.program

object TreeOperations {
  /**
   * Gets a copy of an expression tree where references to the specified argument name are replaced with a new
   * argument name.
   *
   * @param expr         An expression tree.
   * @param originalName The name of the argument to rename.
   * @param newName      The name of the renamed argument in the output tree.
   * @return An expression tree that is identical to the input expression tree but with any references to the original
   *         argument name changed to the new name.
   */
  def renameArgumentInTree(expr: Tree, originalName: String, newName: String): Tree = {
    val newChildren = expr.getChildren.map(child => this.renameArgumentInTree(child, originalName, newName))

    expr match {
      case SelectTerm(name) if name == originalName =>
        SelectTerm(newName)

      case _ =>
        expr.replaceChildren(newChildren.toList)
    }
  }
}
