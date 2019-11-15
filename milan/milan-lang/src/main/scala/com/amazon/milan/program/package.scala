package com.amazon.milan

import com.amazon.milan.typeutil.TypeDescriptor

import scala.language.implicitConversions


package object program {

  implicit class SelectExpressionExtensions(select: SelectExpression) {
    def getRootTermName: String = {
      this.select match {
        case SelectTerm(termName) => termName
        case SelectField(qualifier, _) => qualifier.getRootTermName
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

    /**
     * Replaces the root term of a select expression with a different select expression.
     *
     * @param replacement The replacement for the root term.
     * @return A new [[SelectExpression]] that is equivalent to this one but with the root term replaced.
     */
    def replaceRootTermName(replacement: SelectExpression): SelectExpression = {
      this.select match {
        case _: SelectTerm =>
          replacement

        case SelectField(qualifier, name) =>
          SelectField(qualifier.replaceRootTermName(replacement), name)
      }
    }
  }

  implicit class TypeDescriptorExtensions(ty: TypeDescriptor[_]) {
    /**
     * Gets whether the type being described represents the records on a tuple stream.
     */
    def isTupleStreamType: Boolean = ty.isTuple && ty.fields.nonEmpty
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
