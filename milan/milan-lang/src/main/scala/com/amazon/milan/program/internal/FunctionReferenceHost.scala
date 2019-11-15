package com.amazon.milan.program.internal

import com.amazon.milan.program.FunctionReference

import scala.reflect.macros.whitebox


/**
 * Trait enabling macro bundles to create FunctionReference nodes.
 */
trait FunctionReferenceHost {
  val c: whitebox.Context

  import c.universe._

  private def abort(message: String): Nothing = c.abort(c.enclosingPosition, message)

  /**
   * Gets a [[FunctionReference]] instance, from an AST node.
   * The AST node can be either a Select node, or an Apply node whose function pointer is a Select node.
   *
   * @param tree The AST node that contains the function call.
   * @return A [[FunctionReference]].
   */
  def getFunctionReferenceFromTree(tree: c.universe.Tree): FunctionReference = {
    // We support creating FunctionReference objects from a few types of expressions.
    tree match {
      case Apply(fun, _) =>
        getFunctionReferenceFromTree(fun)

      case TypeApply(fun, _) =>
        getFunctionReferenceFromTree(fun)

      case Select(qualifier, name) =>
        getFunctionReferenceFromSelect(qualifier, name)

      case _ =>
        abort(s"Function expression not supported (only a method call is allowed in the function body): ${showRaw(tree)}")
    }
  }

  /**
   * Gets the full name of the type of a reference.
   *
   * @param ref A reference.
   * @return The full name of the type of the reference.
   */
  def getTypeName(ref: c.universe.Tree): String = {
    ref match {
      case Select(qualifier, TermName(name)) =>
        val qualifierFullName = qualifier.tpe.typeSymbol.fullName
        qualifierFullName + "." + name

      case RefTree(qualifier, name) if qualifier.isEmpty =>
        // This is a reference to a static class, so the full name of the type of the tree is the class name.
        ref.tpe.typeSymbol.fullName

      case _: RefTree =>
        // This is a reference to a static object, so the name is just the string representation of the reference.
        ref.toString()

      case other =>
        abort(s"Function expression not supported: '$other'.")
    }
  }

  /**
   * Gets a [[FunctionReference]] instance, from a Select AST node.
   *
   * @param objectRef    A Select AST node that references the object on which the function is being called.
   * @param functionName The name of the function being called.
   * @return A [[FunctionReference]] instance.
   */
  private def getFunctionReferenceFromSelect(objectRef: c.universe.Tree,
                                             functionName: c.universe.Name): FunctionReference = {
    if (!functionName.isTermName) {
      abort(s"$functionName is not a method name.")
    }

    val typeName = this.getTypeName(objectRef)
    val aggregationTypePrefix = "com.amazon.milan.lang.aggregation."
    val methodName = functionName.toTermName.toString

    if (methodName == "apply" && typeName.startsWith(aggregationTypePrefix)) {
      val actualMethodName = "aggregation." + typeName.substring(aggregationTypePrefix.length)
      FunctionReference("builtin", actualMethodName)
    }
    else {
      FunctionReference(typeName, methodName)
    }
  }
}
