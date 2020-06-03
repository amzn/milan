package com.amazon.milan.flink.internal

import com.amazon.milan.flink.internal.TreeExtractor.{ExtractionContext, ExtractionResult}
import com.amazon.milan.program.{And, Equals, FunctionDef, SelectTerm, Tree}

object JoinKeyExpressionExtractor {
  private val innerExtractor = new TreeExtractor {
    override def extract(tree: Tree, context: ExtractionContext): ExtractionResult = {
      tree match {
        case SelectTerm(termName) if context.hasDifferentArgName(termName) =>
          ExtractionResult(None, Some(tree), context.withArgName(termName))

        case SelectTerm(termName) if !context.hasDifferentArgName(termName) =>
          ExtractionResult(Some(tree), None, context.withArgName(termName))

        case _ =>
          // We support anything as long as only one input argument is referenced in this subtree, and mapRequireAll
          // will enforce that via the combineContexts call.
          this.mapRequireAll(tree.getChildren.toList, tree, context)
      }
    }

    override protected def combineContexts(contexts: List[ExtractionContext]): Option[ExtractionContext] = {
      TreeExtractor.combineContextsIfSameArgumentName(contexts)
    }
  }

  private val topLevelExtractor = new TreeExtractor {
    override def extract(tree: Tree, context: ExtractionContext): ExtractionResult = {
      // At the top level we only allow conjunctions of equality checks.
      // Anything else can't be turned into a KeyAssigner and must be rejected.
      tree match {
        case And(left, right) =>
          // Nested And statements will be treated as a flat list of and-ed statements.
          this.mapSplit(left, right, (l, r) => new And(l, r), context)

        case Equals(left, right) =>
          // Extract from the operands separately. For each one we'll get a context back that will tell us if the
          // operand references an input argument (stream). If the operands reference different arguments then we can
          // use this as part of the key expression.
          val leftResult = innerExtractor.extract(left, context)
          val rightResult = innerExtractor.extract(right, context)

          // If both operands reference different input arguments then we can use this in the key expression.
          if (leftResult.context.argumentName.nonEmpty &&
            rightResult.context.argumentName.nonEmpty &&
            leftResult.context.argumentName != rightResult.context.argumentName) {
            ExtractionResult(Some(tree), None, context)
          }
          else {
            ExtractionResult(None, Some(tree), context)
          }

        case FunctionDef(args, body) =>
          this.map(body, t => new FunctionDef(args, t), context)

        case _ =>
          ExtractionResult(None, Some(tree), context)
      }
    }

    override protected def combineContexts(contexts: List[ExtractionContext]): Option[ExtractionContext] = {
      // We have no requirements on the context at this point.
      // This method is only called in the And branch of the extract method, and all we want to do here is
      // make sure we don't block the mapSplit function which calls here.
      contexts.headOption
    }
  }

  def extractJoinKeyExpression(tree: Tree): TreeExtractionResult = {
    this.topLevelExtractor.extract(tree, ExtractionContext(None)).toTreeExtractionResult
  }
}
