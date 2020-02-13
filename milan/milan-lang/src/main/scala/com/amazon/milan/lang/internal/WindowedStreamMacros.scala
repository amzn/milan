package com.amazon.milan.lang.internal

import com.amazon.milan.Id
import com.amazon.milan.lang.WindowedStream
import com.amazon.milan.program.internal.MappedStreamHost
import com.amazon.milan.program.{GroupingExpression, UniqueBy}
import com.amazon.milan.typeutil.TypeInfoHost

import scala.reflect.macros.whitebox


class WindowedStreamMacros(val c: whitebox.Context) extends TypeInfoHost with FieldStatementHost with MappedStreamHost {

  import c.universe._

  def unique[T: c.WeakTypeTag, TVal: c.WeakTypeTag](selector: c.Expr[T => TVal]): c.Expr[WindowedStream[T]] = {
    val selectFunc = getMilanFunction(selector.tree)
    val outNodeId = Id.newId()

    val inputExprVal = TermName(c.freshName())
    val streamExprVal = TermName(c.freshName())
    val inputRecordTypeVal = TermName(c.freshName())

    val tree =
      q"""
          val $inputExprVal = ${c.prefix}.expr
          val $inputRecordTypeVal = $inputExprVal.recordType
          val $streamExprVal = new ${typeOf[UniqueBy]}($inputExprVal.asInstanceOf[${typeOf[GroupingExpression]}], $selectFunc, $outNodeId, $outNodeId, $inputRecordTypeVal)
          new ${weakTypeOf[WindowedStream[T]]}($streamExprVal)
        """
    c.Expr[WindowedStream[T]](tree)
  }
}
