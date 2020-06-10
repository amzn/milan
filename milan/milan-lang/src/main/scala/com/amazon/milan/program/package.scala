package com.amazon.milan

import scala.language.implicitConversions


package object program {

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
    /**
     * Converts a Milan [[program.Duration]] object to an equivalent [[java.time.Duration]].
     */
    def asJava: java.time.Duration = java.time.Duration.ofMillis(duration.milliseconds)
  }

}
