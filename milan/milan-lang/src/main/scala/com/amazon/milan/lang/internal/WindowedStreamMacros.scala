package com.amazon.milan.lang.internal

import com.amazon.milan.Id
import com.amazon.milan.lang.Stream
import com.amazon.milan.program.WindowApply
import com.amazon.milan.program.internal.{ConvertExpressionHost, MappedStreamHost}
import com.amazon.milan.typeutil.TypeDescriptor

import scala.reflect.macros.whitebox


class WindowedStreamMacros(val c: whitebox.Context)
  extends ConvertExpressionHost
    with MappedStreamHost {

  import c.universe._

  def apply[T: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[Iterable[T] => TOut]): c.Expr[Stream[TOut]] = {
    val applyFunction = getMilanFunction(f.tree)

    val streamType = getStreamTypeExpr[TOut](applyFunction)
    val outputNodeId = Id.newId()

    val sourceExpressionVal = TermName(c.freshName("sourceExpr"))
    val exprVal = TermName(c.freshName("expr"))

    val tree =
      q"""
          val $sourceExpressionVal = ${c.prefix}.expr
          val $exprVal = new ${typeOf[WindowApply]}($sourceExpressionVal, $applyFunction, $outputNodeId, $outputNodeId, $streamType)
          new ${weakTypeOf[Stream[TOut]]}($exprVal, $exprVal.recordType.asInstanceOf[${weakTypeOf[TypeDescriptor[TOut]]}])
       """
    c.Expr[Stream[TOut]](tree)
  }
}
