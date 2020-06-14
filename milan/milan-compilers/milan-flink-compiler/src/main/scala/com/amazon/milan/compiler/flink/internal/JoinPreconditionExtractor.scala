package com.amazon.milan.compiler.flink.internal

import com.amazon.milan.compiler.flink.internal.TreeExtractor.{ExtractionContext, ExtractionResult}
import com.amazon.milan.program.{And, ConstantValue, FunctionDef, IsNull, Not, SelectTerm, Tree}


object JoinPreconditionExtractor {
  private val innerExtractor = new TreeExtractor {
    override def extract(tree: Tree, context: ExtractionContext): ExtractionResult = {
      tree match {
        case And(left, right) =>
          // And can be split across pre and post-conditions and join keys.
          this.mapSplit(left, right, (l, r) => new And(l, r), context)

        case _: ConstantValue =>
          ExtractionResult(Some(tree), None, context)

        case FunctionDef(args, body) =>
          this.map(body, t => new FunctionDef(args, t), context)

        case IsNull(SelectTerm(termName)) =>
          // Null checks on records are post-conditions, because they are checking the state of the join.
          ExtractionResult(None, Some(tree), context.withArgName(termName))

        case IsNull(expr) =>
          this.map(expr, t => new IsNull(t), context)

        case Not(expr) =>
          this.map(expr, t => new Not(t), context)

        case SelectTerm(termName) if context.hasDifferentArgName(termName) =>
          // We found a SelectTerm that references a different argument than one we have already seen in this branch.
          // That means we can't use this branch as a filter before a join, because it requires both streams.
          ExtractionResult(None, Some(tree), context.withArgName(termName))

        case SelectTerm(termName) if !context.hasDifferentArgName(termName) =>
          ExtractionResult(Some(tree), None, context.withArgName(termName))

        case _ =>
          this.mapRequireAll(tree.getChildren.toList, tree, context)
      }
    }

    override protected def combineContexts(contexts: List[ExtractionContext]): Option[ExtractionContext] =
      TreeExtractor.combineContextsIfSameArgumentName(contexts)
  }

  private val topLevelExtractor = new TreeExtractor {
    override def extract(tree: Tree, context: ExtractionContext): ExtractionResult = {
      tree match {
        case And(left, right) =>
          this.mapSplit(left, right, (l, r) => new And(l, r), context)

        case FunctionDef(args, body) =>
          this.map(body, t => new FunctionDef(args, t), context)

        case _ =>
          innerExtractor.extract(tree, context)
      }
    }

    override protected def combineContexts(contexts: List[ExtractionContext]): Option[ExtractionContext] = {
      // We have no requirements on the context at this point.
      // This method is only called in the And branch of the extract method, and all we want to do here is
      // make sure we don't block the mapSplit function which calls here.
      contexts.headOption
    }
  }

  def extractJoinPrecondition(tree: Tree): TreeExtractionResult = {
    this.topLevelExtractor.extract(tree, ExtractionContext(None)).toTreeExtractionResult
  }
}
