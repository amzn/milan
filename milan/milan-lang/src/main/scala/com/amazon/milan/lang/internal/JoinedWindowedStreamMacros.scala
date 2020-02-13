package com.amazon.milan.lang.internal

import com.amazon.milan.Id
import com.amazon.milan.lang.{LeftJoinedWindowedStream, Stream}
import com.amazon.milan.program.internal.ConvertExpressionHost
import com.amazon.milan.program.{FlatMap, FunctionDef, LeftJoin}
import com.amazon.milan.typeutil.{DataStreamTypeDescriptor, TypeDescriptor}

import scala.reflect.macros.whitebox


class JoinedWindowedStreamMacros(val c: whitebox.Context) extends StreamMacroHost with ConvertExpressionHost with FieldStatementHost {

  import c.universe._

  def leftApply[TLeft: c.WeakTypeTag, TRight: c.WeakTypeTag, TOut: c.WeakTypeTag](applyFunction: c.Expr[(TLeft, Iterable[TRight]) => TOut]): c.Expr[Stream[TOut]] = {
    this.warnIfNoRecordId[TOut]()

    val applyFunctionExpr = getMilanFunction(applyFunction.tree)

    val inputStreamVal = TermName(c.freshName())
    val outputTypeInfo = getTypeDescriptor[TOut]

    val tree =
      q"""
          _root_.com.amazon.milan.lang.internal.ProgramValidation.validateFunction($applyFunctionExpr, 2)

          val $inputStreamVal = ${c.prefix}
          _root_.com.amazon.milan.lang.internal.JoinedWindowedStreamUtil.leftApply[${weakTypeOf[TLeft]}, ${weakTypeOf[TRight]}, ${weakTypeOf[TOut]}]($inputStreamVal, $applyFunctionExpr, $outputTypeInfo)
       """
    c.Expr[Stream[TOut]](tree)
  }
}


object JoinedWindowedStreamUtil {
  def leftApply[TLeft, TRight, TOut](inputStream: LeftJoinedWindowedStream[TLeft, TRight],
                                     applyFunction: FunctionDef,
                                     outputRecordType: TypeDescriptor[TOut]): Stream[TOut] = {
    val outNodeId = Id.newId()

    val leftInputExpr = inputStream.leftInput.expr
    val rightInputExpr = inputStream.rightInput.expr

    val joinExpr = new LeftJoin(leftInputExpr, rightInputExpr, null)
    val outputStreamType = new DataStreamTypeDescriptor(outputRecordType)
    val mapExpr = new FlatMap(joinExpr, applyFunction, outNodeId, outNodeId, outputStreamType)

    new Stream[TOut](mapExpr, outputRecordType)
  }
}
