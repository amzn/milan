package com.amazon.milan.lang.internal

import com.amazon.milan.lang.Stream
import com.amazon.milan.program.internal.{ConvertExpressionHost, MappedStreamHost}
import com.amazon.milan.typeutil.TypeDescriptor

import scala.reflect.macros.whitebox


class WindowedStreamMacros(val c: whitebox.Context)
  extends ConvertExpressionHost
    with MappedStreamHost {

  import c.universe._

  def apply[T: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[Iterable[T] => TOut]): c.Expr[Stream[TOut]] = {
    val applyFunction = getMilanFunction(f.tree)
    val streamMapExpr = this.createStreamMap[TOut](applyFunction, q"")

    val exprVal = TermName(c.freshName("expr"))

    val tree =
      q"""
          val $exprVal = $streamMapExpr
          new ${weakTypeOf[Stream[TOut]]}($exprVal, $exprVal.recordType.asInstanceOf[${weakTypeOf[TypeDescriptor[TOut]]}])
       """
    c.Expr[Stream[TOut]](tree)
  }
}
