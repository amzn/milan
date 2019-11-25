package com.amazon.milan.lang.internal

import com.amazon.milan.Id
import com.amazon.milan.lang.{LeftJoinedWindowedStream, ObjectStream}
import com.amazon.milan.program.internal.ConvertExpressionHost
import com.amazon.milan.program.{ComputedStream, FunctionDef, MapRecord, WindowExpression, WindowedLeftJoin}
import com.amazon.milan.types.Record
import com.amazon.milan.typeutil.{StreamTypeDescriptor, TypeDescriptor}

import scala.reflect.macros.whitebox


class JoinedWindowedStreamMacros(val c: whitebox.Context) extends StreamMacroHost with ConvertExpressionHost with FieldStatementHost {

  import c.universe._

  def leftApply[TLeft: c.WeakTypeTag, TRight: c.WeakTypeTag, TOut <: Record : c.WeakTypeTag](applyFunction: c.Expr[(TLeft, Iterable[TRight]) => TOut]): c.Expr[ObjectStream[TOut]] = {
    val applyFunctionExpr = getMilanFunction(applyFunction.tree)

    val inputStreamVal = TermName(c.freshName())
    val outputTypeInfo = getTypeDescriptor[TOut]

    val tree =
      q"""
          com.amazon.milan.lang.internal.ProgramValidation.validateFunction($applyFunctionExpr, 2)

          val $inputStreamVal = ${c.prefix}
          com.amazon.milan.lang.internal.JoinedWindowedStreamUtil.leftApply[${weakTypeOf[TLeft]}, ${weakTypeOf[TRight]}, ${weakTypeOf[TOut]}]($inputStreamVal, $applyFunctionExpr, $outputTypeInfo)
       """
    c.Expr[ObjectStream[TOut]](tree)
  }
}


object JoinedWindowedStreamUtil {
  def leftApply[TLeft, TRight, TOut <: Record](inputStream: LeftJoinedWindowedStream[TLeft, TRight],
                                               applyFunction: FunctionDef,
                                               outputRecordType: TypeDescriptor[TOut]): ObjectStream[TOut] = {
    val outNodeId = Id.newId()

    val leftInputExpr = inputStream.leftInput.node.getStreamExpression
    val rightInputExpr = inputStream.rightInput.node.getExpression.asInstanceOf[WindowExpression]

    val joinExpr = new WindowedLeftJoin(leftInputExpr, rightInputExpr)
    val outputStreamType = new StreamTypeDescriptor(outputRecordType)
    val mapExpr = new MapRecord(joinExpr, applyFunction, outNodeId, outNodeId, outputStreamType)

    val outputNode = ComputedStream(mapExpr.nodeId, mapExpr.nodeName, mapExpr)
    new ObjectStream[TOut](outputNode)
  }
}
