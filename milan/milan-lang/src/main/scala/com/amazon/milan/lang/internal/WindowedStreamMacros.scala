package com.amazon.milan.lang.internal

import com.amazon.milan.Id
import com.amazon.milan.lang.{Stream, WindowedStream}
import com.amazon.milan.program.internal.MappedStreamHost
import com.amazon.milan.program.{ComputedGraphNode, GroupingExpression, UniqueBy}
import com.amazon.milan.typeutil.TypeInfoHost

import scala.reflect.macros.whitebox


class WindowedStreamMacros(val c: whitebox.Context) extends TypeInfoHost with FieldStatementHost with MappedStreamHost with LangTypeNamesHost {

  import c.universe._

  def unique[T: c.WeakTypeTag, TVal: c.WeakTypeTag, TStream <: Stream[T, _] : c.WeakTypeTag](selector: c.Expr[T => TVal]): c.Expr[WindowedStream[T, TStream]] = {
    val inputTypeInfo = createTypeInfo[T]
    val selectFunc = getMilanFunction(selector.tree)
    val outNodeId = Id.newId()
    val streamType = c.weakTypeOf[TStream]

    val inputNodeVal = TermName(c.freshName())
    val streamExprVal = TermName(c.freshName())
    val outNodeVal = TermName(c.freshName())

    val tree =
      q"""
          val $inputNodeVal = ${c.prefix}.node
          val $streamExprVal = new ${typeOf[UniqueBy]}($inputNodeVal.getExpression.asInstanceOf[${typeOf[GroupingExpression]}], $selectFunc, $outNodeId, $outNodeId)
          val $outNodeVal = new ${typeOf[ComputedGraphNode]}($outNodeId, $streamExprVal)
          new ${windowedStreamTypeName(inputTypeInfo.ty, streamType)}($outNodeVal)
       """
    c.Expr[WindowedStream[T, TStream]](tree)
  }
}
