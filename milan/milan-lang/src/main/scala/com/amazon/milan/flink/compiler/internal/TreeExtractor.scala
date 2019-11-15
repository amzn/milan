package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.program.Tree


object TreeExtractor {

  /**
   * Contains information about the context of a tree extraction operation.
   *
   * @param argumentName The name of the function argument (stream) referenced in the tree.
   */
  case class ExtractionContext(argumentName: Option[String]) {
    /**
     * Gets whether this [[ExtractionContext]] already has a different [[argumentName]] assigned than the specified name.
     *
     * @param name The argument name to check for.
     * @return True if this context has an argument name assigned that is different than the specified name, otherwise
     *         false.
     */
    def hasDifferentArgName(name: String): Boolean =
      this.argumentName.nonEmpty && this.argumentName.get != name

    /**
     * Creates a new [[ExtractionContext]] with the same contents as this one, but with the [[argumentName]] set to the
     * specified value.
     *
     * @param name The value of [[argumentName]] to use for the new context object.
     * @return An [[ExtractionContext]] with all values the same as this one, but with the specified [[argumentName]].
     */
    def withArgName(name: String): ExtractionContext =
      ExtractionContext(Some(name))
  }

  /**
   * Contains the result of a tree extraction operation.
   *
   * @param extracted The extracted tree.
   * @param remainder The remainder tree.
   * @param context   A new extraction context resulting from this extraction operation.
   */
  case class ExtractionResult(extracted: Option[Tree], remainder: Option[Tree], context: ExtractionContext) {
    def map(f: Tree => Tree, newContext: ExtractionContext): ExtractionResult = {
      ExtractionResult(this.extracted.map(f), this.remainder.map(f), newContext)
    }

    def toTreeExtractionResult: TreeExtractionResult =
      TreeExtractionResult(this.extracted, this.remainder)
  }

  /**
   * Combine input contexts, if they all have the same argument name.
   *
   * @param contexts A list of [[ExtractionContext]] objects.
   * @return The combined context, or None if the input contexts could not be combined.
   */
  def combineContextsIfSameArgumentName(contexts: List[ExtractionContext]): Option[ExtractionContext] = {
    contexts.filter(_.argumentName.nonEmpty).map(_.argumentName.get).headOption match {
      case Some(argName) if contexts.forall(!_.hasDifferentArgName(argName)) =>
        // At least one context has an argument name, but that name does not conflict with any other context.
        // The combined context is one that has the argument name.
        contexts.find(_.argumentName.nonEmpty)

      case Some(_) =>
        // Two contexts have different argument names, so we can't combine them.
        None

      case None =>
        // No contexts have an argument name so they are all equivalent, just return the first one.
        Some(contexts.head)
    }
  }
}

import com.amazon.milan.flink.compiler.internal.TreeExtractor._


/**
 * Contains the result of a tree extraction operation.
 *
 * @param extracted The extracted tree.
 * @param remainder The remainder tree.
 */
case class TreeExtractionResult(extracted: Option[Tree], remainder: Option[Tree])


/**
 * Base class for tree extractor implementations.
 * Tree extractors walk an expression tree and extract out some subset of the tree.
 * They return the extracted subset as well as the remainder of the tree that was not extracted.
 * Both the extracted subset and the remainder that are returned will be valid expression trees.
 */
abstract class TreeExtractor {
  def extract(tree: Tree, context: ExtractionContext): ExtractionResult

  protected def combineContexts(contexts: List[ExtractionContext]): Option[ExtractionContext]

  /**
   * Perform the extraction on the specified trees. If all of the extractions succeed with no remainder, and the
   * resulting extraction contexts can be combined, sthen return the specified output tree as the extracted output.
   * Otherwise, return the output tree as the remainder.
   *
   * @param trees   The trees to perform extraction on.
   * @param output  The output tree.
   * @param context The current extraction context.
   * @return An [[ExtractionResult]] containing the output tree, either as the extracted result or the remainder.
   */
  protected def mapRequireAll(trees: List[Tree],
                              output: Tree,
                              context: ExtractionContext): ExtractionResult = {
    if (trees.isEmpty) {
      ExtractionResult(Some(output), None, context)
    }
    else {
      val results = trees.map(t => this.extract(t, context))

      // Combine the contexts if they are compatible.
      // If they aren't then we have to put everything into the remainder.
      this.combineContexts(results.map(_.context)) match {
        case None =>
          // We couldn't combine the contexts from the recursive extraction, so we return the output as the remainder
          // with the input context.
          // Returning the input context is fine because the returned context is only relevant for successful
          // extractions.
          ExtractionResult(None, Some(output), context)

        case Some(newContext) =>
          // If we extracted something from all of the trees with no remainder then we can return the output
          // as the extracted tree. Otherwise the output goes into the remainder.
          if (results.forall(_.remainder.isEmpty) && results.forall(_.extracted.nonEmpty)) {
            ExtractionResult(Some(output), None, newContext)
          }
          else {
            ExtractionResult(None, Some(output), newContext)
          }
      }
    }
  }

  /**
   * Perform extraction on the specified tree, and map the resulting extracted and remainder trees through a function.
   *
   * @param tree    The tree to perform extraction on.
   * @param f       A map function for the extracted and remainder trees.
   * @param context The current extraction context.
   * @return An [[ExtractionResult]] containing the mapped extracted and remainder trees.
   */
  protected def map(tree: Tree, f: Tree => Tree, context: ExtractionContext): ExtractionResult = {
    this.extract(tree, context).map(f, context)
  }

  /**
   * Performs extraction on the specified trees, and maps the resulting extracted and remainder trees though map
   * functions, depending on whether both trees produce extracted and/or remainder trees.
   *
   * @param t1      A tree to perform extraction on.
   * @param t2      A tree to perform extraction on.
   * @param f       A map function to use when both input trees produce an extracted or remainder result.
   * @param context The current extraction context.
   * @return An [[ExtractionResult]] containing the mapped results.
   */
  protected def mapSplit(t1: Tree,
                         t2: Tree,
                         f: (Tree, Tree) => Tree,
                         context: ExtractionContext): ExtractionResult = {
    // First extract from the input trees.
    val r1 = this.extract(t1, context)
    val r2 = this.extract(t2, context)

    val (extracted, newContext: ExtractionContext) =
      this.getExtractedFromResults(r1, r2, f, context)

    val remainder =
      (r1.remainder, r2.remainder) match {
        case (Some(rt1), Some(rt2)) => Some(f(rt1, rt2))
        case (Some(rt1), None) => Some(rt1)
        case (None, Some(rt2)) => Some(rt2)
        case (None, None) => None
      }

    ExtractionResult(extracted, remainder, newContext)
  }

  /**
   * Gets the extracted tree from a set of extraction results.
   * The extracted trees from the input results are mapped through map functions, depending on whether both trees
   * produce extracted trees and whether the result contexts can be combined.
   *
   * @param r1      The first extraction result.
   * @param r2      The second extraction result.
   * @param f       A map function to use when both results contain an extracted tree.
   * @param context The current extraction context.
   * @return A tuple of the result of the output of the appropriate map function, and the combined contexts.
   */
  private def getExtractedFromResults(r1: ExtractionResult,
                                      r2: ExtractionResult,
                                      f: (Tree, Tree) => Tree,
                                      context: ExtractionContext): (Option[Tree], ExtractionContext) = {
    (r1.extracted, r2.extracted, this.combineContexts(List(r1.context, r2.context))) match {
      case (Some(e1), Some(e2), Some(c)) =>
        // We got an extracted result from both inputs and the resulting contexts are compatible, so map the
        // extracted trees through the map function.
        (Some(f(e1, e2)), c)

      case (Some(e1), None, _) =>
        // We got an extracted result from only the first input, so map it.
        (Some(e1), r1.context)

      case (None, Some(e2), _) =>
        // We got an extracted result from only the second input, so map it.
        (Some(e2), r2.context)

      case _ =>
        // Neither tree produced an extracted result, or the resulting contexts were incompatible.
        (None: Option[Tree], context)
    }
  }
}
