package com.amazon.milan.lang.internal

import com.amazon.milan.Id
import com.amazon.milan.lang.{LeftJoinedWindowedStream, Stream}
import com.amazon.milan.program.internal.ConvertExpressionHost
import com.amazon.milan.program.{ComputedStream, FunctionDef, MapRecord, WindowExpression, WindowedLeftJoin}
import com.amazon.milan.typeutil.{StreamTypeDescriptor, TypeDescriptor}

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

    val leftInputExpr = inputStream.leftInput.node.getStreamExpression
    val rightInputExpr = inputStream.rightInput.node.getExpression.asInstanceOf[WindowExpression]

    val joinExpr = new WindowedLeftJoin(leftInputExpr, rightInputExpr)
    val outputStreamType = new StreamTypeDescriptor(outputRecordType)
    val mapExpr = new MapRecord(joinExpr, applyFunction, outNodeId, outNodeId, outputStreamType)

    val outputNode = ComputedStream(mapExpr.nodeId, mapExpr.nodeName, mapExpr)
    new Stream[TOut](outputNode, outputRecordType)
  }
}
