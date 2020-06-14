package com.amazon.milan.compiler.flink.internal

import com.amazon.milan.compiler.scala.TypeEmitter
import com.amazon.milan.compiler.flink.TypeUtil
import com.amazon.milan.compiler.flink.types.ArrayRecord
import com.amazon.milan.compiler.flink.typeutil.TupleRecordTypeDescriptor
import com.amazon.milan.typeutil.{TupleTypeDescriptor, TypeDescriptor}


class FlinkTypeEmitter extends TypeEmitter {
  override def getTypeFullName(ty: TypeDescriptor[_]): String = {
    ty match {
      case _: TupleRecordTypeDescriptor[_] =>
        ArrayRecord.typeName

      case _: TupleTypeDescriptor[_] =>
        val className = TypeUtil.getTupleClassName(ty.genericArguments.length)
        if (ty.genericArguments.isEmpty) {
          className
        }
        else {
          className + ty.genericArguments.map(this.getTypeFullName).mkString("[", ", ", "]")
        }

      case _ if ty.genericArguments.nonEmpty =>
        ty.typeName + ty.genericArguments.map(this.getTypeFullName).mkString("[", ", ", "]")

      case _ =>
        ty.fullName
    }
  }

  override def getTupleElementField(index: Int): String = "_" + (index + 1).toString

  override def getTupleClassName(elementCount: Int): String = TypeUtil.getTupleClassName(elementCount)
}
