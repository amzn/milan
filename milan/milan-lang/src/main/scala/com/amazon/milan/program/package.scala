package com.amazon.milan

import scala.language.implicitConversions


package object program {

  implicit class SelectExpressionExtensions(select: SelectExpression) {
    /**
     * Gets the root qualifier in a series of nested Select expressions.
     *
     * @return The first SelectField qualifier that is not a SelectField expression.
     */
    def getRootQualifier: Tree = {
      this.select match {
        case SelectField(qualifier: SelectField, _) => qualifier.getRootQualifier
        case root => root
      }
    }

    /**
     * Gets a new [[SelectExpression]] that is equivalent to this expression but with the root term removed.
     * For example, if this expression is A.B.C.D, this method will return the expression B.C.D
     */
    def trimRootTermName(): SelectExpression = {
      this.select match {
        case _: SelectTerm =>
          throw new IllegalArgumentException()

        case SelectField(qualifier, name) =>
          qualifier match {
            case _: SelectTerm =>
              SelectTerm(name)

            case f: SelectField =>
              SelectField(f.trimRootTermName(), name)
          }
      }
    }
  }

  implicit class TreeExtensions(tree: Tree) {
    /**
     * Gets all nodes in an expression tree for which a predicate returns true.
     */
    def search(predicate: Tree => Boolean): Iterable[Tree] = {
      val childResults = this.tree.getChildren.flatMap(child => child.search(predicate))

      val thisResult =
        if (predicate(this.tree)) {
          Seq(this.tree)
        }
        else {
          Seq.empty
        }

      childResults ++ thisResult
    }

    /**
     * Gets the names from all [[SelectTerm]] nodes in an expression tree.
     */
    def listTermNames(): Iterable[String] = {
      this.search(t => t.isInstanceOf[SelectTerm]).map(_.asInstanceOf[SelectTerm].termName)
    }
  }

  implicit class DurationExtensions(duration: program.Duration) {
    def asJava: java.time.Duration = java.time.Duration.ofMillis(duration.milliseconds)
  }

}
