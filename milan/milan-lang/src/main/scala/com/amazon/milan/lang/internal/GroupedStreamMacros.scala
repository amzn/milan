package com.amazon.milan.lang.internal

import java.util.UUID

import com.amazon.milan.lang.{GroupedStream, Stream}
import com.amazon.milan.program.{FlatMap, FunctionDef, Marker, SelectTerm, StreamMap, ValueDef}
import com.amazon.milan.typeutil.{TypeDescriptor, TypeInfoHost}
import com.amazon.milan.{Id, program}

import scala.reflect.macros.whitebox


class GroupedStreamMacros(val c: whitebox.Context) extends StreamMacroHost with TypeInfoHost {

  import c.universe._

  def unkeyedSelectObject[T: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[T => TOut]): c.Expr[Stream[TOut]] = {
    this.warnIfNoRecordId[TOut]()

    val aggregateExpression = getMilanFunction(f.tree)
    val validationExpression = q"com.amazon.milan.program.ProgramValidation.validateMapFunction($aggregateExpression, 1)"
    val exprTree = this.createAggregate[TOut](aggregateExpression, validationExpression)

    val exprVal = TermName(c.freshName("expr"))

    val tree =
      q"""
          val $exprVal = $exprTree
          new ${weakTypeOf[Stream[TOut]]}($exprVal, $exprVal.recordType)
       """
    c.Expr[Stream[TOut]](tree)
  }

  def keyedSelectObject[T: c.WeakTypeTag, TKey: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[(TKey, T) => TOut]): c.Expr[Stream[TOut]] = {
    this.warnIfNoRecordId[TOut]()

    val aggregateExpression = getMilanFunction(f.tree)
    val validationExpression =
      q"""
          com.amazon.milan.program.ProgramValidation.validateMapFunction($aggregateExpression, 2)
          com.amazon.milan.lang.internal.ProgramValidation.validateSelectFromGroupByFunction($aggregateExpression)
       """

    val exprTree = this.createAggregate[TOut](aggregateExpression, validationExpression)

    val exprVal = TermName(c.freshName("expr"))

    val tree =
      q"""
         val $exprVal = $exprTree
         new ${weakTypeOf[Stream[TOut]]}($exprVal, $exprVal.recordType.asInstanceOf[${weakTypeOf[TypeDescriptor[TOut]]}])
       """

    c.Expr[Stream[TOut]](tree)
  }

  def map[T: c.WeakTypeTag, TKey: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[(TKey, Stream[T]) => Stream[TOut]]): c.Expr[GroupedStream[TOut, TKey]] = {
    this.warnIfNoRecordId[TOut]()

    def generateValidationExpression(mapFunctionDef: c.Expr[FunctionDef]): c.universe.Tree = {
      q"com.amazon.milan.lang.internal.ProgramValidation.validateSelectFromGroupByFunction($mapFunctionDef)"
    }

    val mapExpr = this.rewriteKeyedMapStreamExpression(f)
    val sourceVal = TermName(c.freshName("source"))
    val exprVal = TermName(c.freshName("expr"))

    val streamType = getGroupedStreamTypeExpr[TOut](mapExpr)
    val id = Id.newId()

    val tree =
      q"""
          val $sourceVal = ${c.prefix}
          val $exprVal = new ${typeOf[StreamMap]}($sourceVal.expr, $mapExpr, $id, $id, $streamType)
          new ${weakTypeOf[GroupedStream[TOut, TKey]]}($exprVal)
       """
    c.Expr[GroupedStream[TOut, TKey]](tree)
  }

  def unkeyedFlatMap[T: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[Stream[T] => Stream[TOut]]): c.Expr[Stream[TOut]] = {
    this.warnIfNoRecordId[TOut]()

    val mapExpr = this.rewriteUnkeyedMapStreamExpression(f)
    this.flatMap[TOut](mapExpr)
  }

  def keyedFlatMap[T: c.WeakTypeTag, TKey: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[(TKey, Stream[T]) => Stream[TOut]]): c.Expr[Stream[TOut]] = {
    this.warnIfNoRecordId[TOut]()

    val mapExpr = this.rewriteKeyedMapStreamExpression(f)
    this.flatMap[TOut](mapExpr)
  }

  def flatMapSingle[T: c.WeakTypeTag, TKey: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[(TKey, Stream[T]) => TOut]): c.Expr[Stream[TOut]] = {
    c.Expr[Stream[TOut]](q"")
  }

  private def flatMap[TOut: c.WeakTypeTag](mapExpr: c.Expr[FunctionDef]): c.Expr[Stream[TOut]] = {
    val sourceVal = TermName(c.freshName("source"))
    val exprVal = TermName(c.freshName("expr"))
    val streamType = getStreamTypeExpr[TOut](mapExpr)
    val id = Id.newId()

    val tree =
      q"""
          val $sourceVal = ${c.prefix}
          val $exprVal = new ${typeOf[FlatMap]}($sourceVal.expr, $mapExpr, $id, $id, $streamType)
          new ${weakTypeOf[Stream[TOut]]}($exprVal, $exprVal.recordType.asInstanceOf[${weakTypeOf[TypeDescriptor[TOut]]}])
       """

    c.Expr[Stream[TOut]](tree)
  }

  private def rewriteUnkeyedMapStreamExpression[T: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[Stream[T] => Stream[TOut]]): c.Expr[FunctionDef] = {
    this.rewriteMapStreamExpression[T](f.tree, 0)
  }

  private def rewriteKeyedMapStreamExpression[TKey: c.WeakTypeTag, T: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[(TKey, Stream[T]) => Stream[TOut]]): c.Expr[FunctionDef] = {
    this.rewriteMapStreamExpression[T](f.tree, 1)
  }

  private def rewriteMapStreamExpression[T: c.WeakTypeTag](f: c.Tree, streamArgIndex: Int): c.Expr[FunctionDef] = {
    val Function(args, body) = f

    val argNames = args.map(_.name.toString)
    val streamArgName = argNames(streamArgIndex)

    // We need to create a Stream[T] to pass to the body of the map function in order to get back the Milan AST
    // of the map expression. We use a Marker object so that we can later figure out where to plug in the the generated
    // AST.
    val markerId = UUID.randomUUID().toString
    val recordType = this.getTypeDescriptor[T]
    val inputStreamExpr = q"new ${typeOf[Marker]}($markerId, ${recordType.toDataStream})"
    val inputStream = q"new ${weakTypeOf[Stream[T]]}($inputStreamExpr, $recordType)"

    //    c.warning(c.enclosingPosition, s"Old Body: $body")
    //    c.warning(c.enclosingPosition, s"Old Body: ${showRaw(body)}")

    val newBody = this.rewriteMapStreamFunctionBody(body, streamArgName, inputStream)

    //    c.warning(c.enclosingPosition, s"New Body: $newBody")
    //    c.warning(c.enclosingPosition, s"New Body: ${showRaw(newBody)}")

    val argDefs = argNames.map(ValueDef.named)
    val tree = q"new ${typeOf[FunctionDef]}($argDefs, _root_.com.amazon.milan.lang.internal.GroupedStreamUtil.replaceMarkerWithArg($newBody.expr, $markerId, $streamArgName))"

    c.Expr[FunctionDef](tree)
  }

  /**
   * Rewrites the scala AST of the body of an anonymous function, replacing references to a specified function argument
   * with a different tree.
   *
   * @param body            The AST of a function body.
   * @param streamArgName   The name of the function argument whose references are to be replaced.
   * @param inputStreamExpr The tree to replace the argument references with.
   * @return A copy of the input tree with the argument references replaced.
   */
  private def rewriteMapStreamFunctionBody(body: Tree, streamArgName: String, inputStreamExpr: Tree): Tree = {
    def rewrite(tree: Tree): Tree =
      this.rewriteMapStreamFunctionBody(tree, streamArgName, inputStreamExpr)

    def rewriteList(trees: List[Tree]): List[Tree] =
      trees.map(rewrite)

    // Rewriting the tree to replace argument references is not easy because Scala doesn't provide the "replace children"
    // operation that Milan trees do. We have to traverse the tree by conditioning on every specific tree type
    // that we want to support.
    body match {
      case Apply(fun, args) => Apply(rewrite(fun), rewriteList(args))
      case Ident(TermName(name)) if name == streamArgName => inputStreamExpr
      case Block(stats, expr) => Block(rewriteList(stats), rewrite(expr))
      case Select(qualifier, name) => Select(rewrite(qualifier), name)
      case TypeApply(fun, args) => TypeApply(fun, rewriteList(args))
      case ValDef(mods, name, tpt, rhs) => ValDef(mods, name, tpt, rewrite(rhs))
      case other => other
    }
  }
}


object GroupedStreamUtil {
  def replaceMarkerWithArg(expr: program.Tree, markerId: String, argName: String): program.Tree = {
    expr match {
      case Marker(id) if id == markerId =>
        SelectTerm(argName)

      case _ =>
        val newChildren = expr.getChildren.map(child => this.replaceMarkerWithArg(child, markerId, argName)).toList
        expr.replaceChildren(newChildren)
    }
  }
}
