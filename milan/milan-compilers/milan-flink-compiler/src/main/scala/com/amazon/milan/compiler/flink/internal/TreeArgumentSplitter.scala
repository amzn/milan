package com.amazon.milan.compiler.flink.internal

import com.amazon.milan.compiler.flink.internal.TreeExtractor.{ExtractionContext, ExtractionResult}
import com.amazon.milan.program.{And, FunctionDef, SelectTerm, Tree}


object TreeArgumentSplitter {
  /**
   * Extracts a subtree that only references the specified argument.
   *
   * @param tree    The tree to split.
   * @param argName The name of the target argument.
   * @return A [[TreeExtractionResult]]. The extracted tree is the portion of the tree that references only the
   *         specified argument. The remainder is the portion that references other arguments.
   */
  def splitTree(tree: Tree, argName: String): TreeExtractionResult = {
    new Extractor(argName).extract(tree).toTreeExtractionResult
  }

  class Extractor(extractArgName: String) extends TreeExtractor {
    def extract(tree: Tree): ExtractionResult = {
      this.extract(tree, ExtractionContext(None))
    }

    override def extract(tree: Tree, context: ExtractionContext): ExtractionResult = {
      tree match {
        case And(left, right) =>
          this.mapSplit(left, right, (l, r) => new And(l, r), context)

        case FunctionDef(args, body) =>
          this.map(body, t => new FunctionDef(args, t), context)

        case SelectTerm(argName) if argName == this.extractArgName =>
          ExtractionResult(Some(tree), None, context)

        case SelectTerm(argName) if argName != this.extractArgName =>
          ExtractionResult(None, Some(tree), context)

        case _ =>
          this.mapRequireAll(tree.getChildren.toList, tree, context)
      }
    }

    override protected def combineContexts(contexts: List[ExtractionContext]): Option[ExtractionContext] = {
      // The context is not interesting to us because we filter for argument names in the extract method.
      contexts.headOption
    }
  }

}
