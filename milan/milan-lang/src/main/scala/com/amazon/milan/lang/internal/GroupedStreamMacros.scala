package com.amazon.milan.lang.internal

import com.amazon.milan.Id
import com.amazon.milan.lang.{Function2FieldStatement, GroupedStream, Stream}
import com.amazon.milan.program.{ArgMax, ComputedGraphNode, ComputedStream, FunctionDef, GraphNodeExpression, GroupingExpression, MapFields, MapRecord, SelectTerm, StreamExpression, TimeWindowExpression, Tuple, UniqueBy}
import com.amazon.milan.typeutil.{StreamTypeDescriptor, TypeDescriptor, TypeInfoHost}

import scala.annotation.tailrec
import scala.reflect.macros.whitebox


class GroupedStreamMacros(val c: whitebox.Context) extends StreamMacroHost with TypeInfoHost with FieldStatementHost {

  import c.universe._

  def unique[T: c.WeakTypeTag, TKey: c.WeakTypeTag, TVal: c.WeakTypeTag](selector: c.Expr[T => TVal]): c.Expr[GroupedStream[T, TKey]] = {
    val selectFunc = getMilanFunction(selector.tree)
    val outNodeId = Id.newId()

    val inputNodeVal = TermName(c.freshName())
    val streamExprVal = TermName(c.freshName())
    val outNodeVal = TermName(c.freshName())

    val tree =
      q"""
          val $inputNodeVal = ${c.prefix}.node
          val $streamExprVal = new ${typeOf[UniqueBy]}($inputNodeVal.getExpression.asInstanceOf[${typeOf[GroupingExpression]}], $selectFunc, $outNodeId, $outNodeId)
          val $outNodeVal = new ${typeOf[ComputedGraphNode]}($outNodeId, $streamExprVal)
          new ${weakTypeOf[GroupedStream[T, TKey]]}($outNodeVal)
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
    val recordTypeVal = TermName(c.freshName())
    val nodeVal = TermName(c.freshName())

    val tree =
      q"""
          val $inputExprVal = ${c.prefix}.node.getExpression
          val $nodeVal = _root_.com.amazon.milan.lang.internal.GroupedStreamUtil.getMaxByStreamNode($inputExprVal, $argFunctionDef)
          val $recordTypeVal = _root_.com.amazon.milan.lang.internal.GroupedStreamUtil.getRecordType($inputExprVal).asInstanceOf[${weakTypeOf[TypeDescriptor[T]]}]
          new ${c.weakTypeOf[Stream[T]]}($nodeVal, $recordTypeVal)
       """

    c.Expr[Stream[T]](tree)
  }
}


object GroupedStreamUtil {
  def getMaxByStreamNode(sourceExpr: GraphNodeExpression,
                         argFunctionDef: FunctionDef): ComputedStream = {
    val inputRecordType = this.getRecordType(sourceExpr)

    val mapExpr =
      if (inputRecordType.isTuple) {
        this.getMaxByMapFieldsExpression(sourceExpr, inputRecordType, argFunctionDef)
      }
      else {
        this.getMaxByMapRecordExpression(sourceExpr, inputRecordType, argFunctionDef)
      }

    ComputedStream(mapExpr.nodeId, mapExpr.nodeName, mapExpr)
  }

  @tailrec
  def getRecordType(expr: GraphNodeExpression): TypeDescriptor[_] = {
    expr match {
      case s: StreamExpression => s.tpe.asStream.recordType
      case GroupingExpression(source, _) => this.getRecordType(source)
      case TimeWindowExpression(source, _, _, _) => this.getRecordType(source)
    }
  }

  private def getMaxByMapFieldsExpression(sourceExpr: GraphNodeExpression,
                                          inputRecordType: TypeDescriptor[_],
                                          argFunctionDef: FunctionDef): MapFields = {
    throw new NotImplementedError()
  }

  private def getMaxByMapRecordExpression(sourceExpr: GraphNodeExpression,
                                          inputRecordType: TypeDescriptor[_],
                                          argFunctionDef: FunctionDef): MapRecord = {
    val argName = argFunctionDef.arguments.head
    val mapFunctionDef = FunctionDef(List("_", argName), ArgMax(Tuple(List(argFunctionDef.expr, SelectTerm(argName)))))
    val id = Id.newId()
    new MapRecord(sourceExpr, mapFunctionDef, id, id, new StreamTypeDescriptor(inputRecordType))
  }
}
