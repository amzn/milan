package com.amazon.milan.lang.internal

import java.util.UUID

import com.amazon.milan.lang.{Function2FieldStatement, GroupedStream, Stream}
import com.amazon.milan.program.{ArgMax, FunctionDef, GroupingExpression, MapExpression, MapFields, MapRecord, Marker, SelectTerm, StreamExpression, Tuple, UniqueBy}
import com.amazon.milan.typeutil.{DataStreamTypeDescriptor, TypeDescriptor, TypeInfoHost}
import com.amazon.milan.{Id, program}

import scala.reflect.macros.whitebox


class GroupedStreamMacros(val c: whitebox.Context) extends StreamMacroHost with TypeInfoHost with FieldStatementHost {

  import c.universe._

  def unique[T: c.WeakTypeTag, TKey: c.WeakTypeTag, TVal: c.WeakTypeTag](selector: c.Expr[T => TVal]): c.Expr[GroupedStream[T, TKey]] = {
    val selectFunc = getMilanFunction(selector.tree)
    val outNodeId = Id.newId()

    val inputExprVal = TermName(c.freshName())
    val streamExprVal = TermName(c.freshName())

    val tree =
      q"""
          val $inputExprVal = ${c.prefix}.expr
          val $streamExprVal = new ${typeOf[UniqueBy]}($inputExprVal.asInstanceOf[${typeOf[GroupingExpression]}], $selectFunc, $outNodeId, $outNodeId)
          new ${weakTypeOf[GroupedStream[T, TKey]]}($streamExprVal)
       """
    c.Expr[GroupedStream[T, TKey]](tree)
  }

  def selectObject[T: c.WeakTypeTag, TKey: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[(TKey, T) => TOut]): c.Expr[Stream[TOut]] = {
    this.warnIfNoRecordId[TOut]()

    def generateValidationExpression(mapFunctionDef: c.Expr[FunctionDef]): c.universe.Tree = {
      q"com.amazon.milan.lang.internal.ProgramValidation.validateSelectFromGroupByFunction($mapFunctionDef)"
    }

    val nodeTree = createMappedToRecordStream2[TKey, T, TOut](f, generateValidationExpression)
    val outputRecordType = createTypeInfo[TOut].toTypeDescriptor
    val tree = q"new ${weakTypeOf[Stream[TOut]]}($nodeTree, $outputRecordType)"
    c.Expr[Stream[TOut]](tree)
  }

  def selectTuple1[T: c.WeakTypeTag, TKey: c.WeakTypeTag, TF: c.WeakTypeTag](f: c.Expr[Function2FieldStatement[TKey, T, TF]]): c.Expr[Stream[Tuple1[TF]]] = {
    val streamTypeInfo = createTypeInfo[T]
    val keyTypeInfo = createTypeInfo[TKey]
    val expr = getFieldDefinitionForSelectFromGroupBy[T, TKey, TF](streamTypeInfo, keyTypeInfo, f)
    mapTuple[Tuple1[TF]](List((expr, c.weakTypeOf[TF])))
  }

  def selectTuple2[T: c.WeakTypeTag, TKey: c.WeakTypeTag, T1: c.WeakTypeTag, T2: c.WeakTypeTag](f1: c.Expr[Function2FieldStatement[TKey, T, T1]],
                                                                                                f2: c.Expr[Function2FieldStatement[TKey, T, T2]]): c.Expr[Stream[(T1, T2)]] = {
    val streamTypeInfo = createTypeInfo[T]
    val keyTypeInfo = createTypeInfo[TKey]
    val expr1 = getFieldDefinitionForSelectFromGroupBy[T, TKey, T1](streamTypeInfo, keyTypeInfo, f1)
    val expr2 = getFieldDefinitionForSelectFromGroupBy[T, TKey, T2](streamTypeInfo, keyTypeInfo, f2)

    val fields = List(
      (expr1, c.weakTypeOf[T1]),
      (expr2, c.weakTypeOf[T2])
    )
    mapTuple[(T1, T2)](fields)
  }

  def selectTuple3[T: c.WeakTypeTag, TKey: c.WeakTypeTag, T1: c.WeakTypeTag, T2: c.WeakTypeTag, T3: c.WeakTypeTag](f1: c.Expr[Function2FieldStatement[TKey, T, T1]],
                                                                                                                   f2: c.Expr[Function2FieldStatement[TKey, T, T2]],
                                                                                                                   f3: c.Expr[Function2FieldStatement[TKey, T, T3]]): c.Expr[Stream[(T1, T2, T3)]] = {
    val streamTypeInfo = createTypeInfo[T]
    val keyTypeInfo = createTypeInfo[TKey]
    val expr1 = getFieldDefinitionForSelectFromGroupBy[T, TKey, T1](streamTypeInfo, keyTypeInfo, f1)
    val expr2 = getFieldDefinitionForSelectFromGroupBy[T, TKey, T2](streamTypeInfo, keyTypeInfo, f2)
    val expr3 = getFieldDefinitionForSelectFromGroupBy[T, TKey, T3](streamTypeInfo, keyTypeInfo, f3)

    val fields = List(
      (expr1, c.weakTypeOf[T1]),
      (expr2, c.weakTypeOf[T2]),
      (expr3, c.weakTypeOf[T3])
    )

    mapTuple[(T1, T2, T3)](fields)
  }

  def maxBy[T: c.WeakTypeTag, TArg: c.WeakTypeTag](f: c.Expr[T => TArg])
                                                  (ev: c.Expr[Ordering[TArg]]): c.Expr[Stream[T]] = {
    // MaxBy is essentially syntactic sugar for select((_, r) => argmax(f(r), r))
    val argFunctionDef = getMilanFunction(f.tree)

    val inputExprVal = TermName(c.freshName())
    val mapExprVal = TermName(c.freshName())
    val recordTypeVal = TermName(c.freshName())

    val tree =
      q"""
          val $inputExprVal = ${c.prefix}.expr
          val $mapExprVal = _root_.com.amazon.milan.lang.internal.GroupedStreamUtil.getMaxByMapExpression($inputExprVal, $argFunctionDef)
          val $recordTypeVal = $mapExprVal.recordType.asInstanceOf[${weakTypeOf[TypeDescriptor[T]]}]
          new ${c.weakTypeOf[Stream[T]]}($mapExprVal, $recordTypeVal)
       """

    c.Expr[Stream[T]](tree)
  }

  def map[T: c.WeakTypeTag, TKey: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[(TKey, Stream[T]) => Stream[TOut]]): c.Expr[GroupedStream[TOut, TKey]] = {
    this.warnIfNoRecordId[TOut]()

    def generateValidationExpression(mapFunctionDef: c.Expr[FunctionDef]): c.universe.Tree = {
      q"com.amazon.milan.lang.internal.ProgramValidation.validateSelectFromGroupByFunction($mapFunctionDef)"
    }

    val mapExpr = this.rewriteMapStreamExpression(f, 1)
    val sourceVal = TermName(c.freshName())
    val exprVal = TermName(c.freshName())

    val tree =
      q"""
          val $sourceVal = ${c.prefix}
          val $exprVal = new ${typeOf[MapRecord]}($sourceVal.expr, $mapExpr)
          new ${weakTypeOf[GroupedStream[TOut, TKey]]}($exprVal)
       """
    c.Expr[GroupedStream[TOut, TKey]](tree)
  }

  def flatMap[T: c.WeakTypeTag, TKey: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[(TKey, Stream[T]) => Stream[TOut]]): c.Expr[Stream[TOut]] = {
    this.warnIfNoRecordId[TOut]()

    val mapFunctionExpr = getMilanFunction(f.tree)
    val outputRecordType = getTypeDescriptor[TOut]

    val mapExprVal = TermName(c.freshName())

    val tree =
      q"""
          val $mapExprVal = $mapFunctionExpr
          $mapExprVal.tpe = $outputRecordType
          new ${weakTypeOf[Stream[TOut]]}($mapExprVal, $outputRecordType)
       """

    c.Expr[Stream[TOut]](tree)
  }

  def flatMapSingle[T: c.WeakTypeTag, TKey: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[(TKey, Stream[T]) => TOut]): c.Expr[Stream[TOut]] = {
    c.Expr[Stream[TOut]](q"")
  }

  private def rewriteMapStreamExpression[TKey: c.WeakTypeTag, T: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[(TKey, Stream[T]) => Stream[TOut]],
                                                                                                     streamArgIndex: Int): c.Expr[FunctionDef] = {
    val Function(args, body) = f.tree

    val argNames = args.map(_.name.toString)
    val streamArgName = argNames(streamArgIndex)

    // We need to create a Stream[T] to pass to the body of the map function in order to get back the Milan AST
    // of the map expression.
    val markerId = UUID.randomUUID().toString
    val inputStreamExpr = q"new ${typeOf[Marker]}($markerId)"
    val recordType = this.getTypeDescriptor[T]
    val inputStream = q"new ${weakTypeOf[Stream[T]]}($inputStreamExpr, $recordType)"
    val newBody = this.rewriteMapStreamFunctionBody(body, streamArgName, inputStream)

    val tree = q"new ${typeOf[FunctionDef]}($argNames, _root_.com.amazon.milan.lang.internal.GroupedStreamUtil.replaceMarkerWithArg($newBody.expr, $markerId, $streamArgName))"

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
    def rewrite(args: List[Tree]): List[Tree] =
      args.map(arg => this.rewriteMapStreamFunctionBody(arg, streamArgName, inputStreamExpr))

    body match {
      case Ident(TermName(name)) if name == streamArgName => inputStreamExpr
      case Apply(fun, args) => Apply(fun, rewrite(args))
      case other => other
    }
  }
}


object GroupedStreamUtil {
  def replaceMarkerWithArg(expr: program.Tree, markerId: String, argName: String): program.Tree = {
    println(s"$markerId -> $expr")

    expr match {
      case Marker(id) if id == markerId =>
        SelectTerm(argName)

      case _ =>
        val newChildren = expr.getChildren.map(child => this.replaceMarkerWithArg(child, markerId, argName)).toList
        expr.replaceChildren(newChildren)
    }
  }

  def getMaxByMapExpression(sourceExpr: StreamExpression,
                            argFunctionDef: FunctionDef): MapExpression = {
    val inputRecordType = sourceExpr.recordType

    val mapExpr =
      if (inputRecordType.isTuple) {
        this.getMaxByMapFieldsExpression(sourceExpr, inputRecordType, argFunctionDef)
      }
      else {
        this.getMaxByMapRecordExpression(sourceExpr, inputRecordType, argFunctionDef)
      }

    mapExpr
  }

  private def getMaxByMapFieldsExpression(sourceExpr: StreamExpression,
                                          inputRecordType: TypeDescriptor[_],
                                          argFunctionDef: FunctionDef): MapFields = {
    throw new NotImplementedError()
  }

  private def getMaxByMapRecordExpression(sourceExpr: StreamExpression,
                                          inputRecordType: TypeDescriptor[_],
                                          argFunctionDef: FunctionDef): MapRecord = {
    val argName = argFunctionDef.arguments.head
    val mapFunctionDef = FunctionDef(List("_", argName), ArgMax(Tuple(List(argFunctionDef.expr, SelectTerm(argName)))))
    val id = Id.newId()
    new MapRecord(sourceExpr, mapFunctionDef, id, id, new DataStreamTypeDescriptor(inputRecordType))
  }
}
