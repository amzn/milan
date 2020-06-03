package com.amazon.milan.flink.internal

import com.amazon.milan.flink.TypeUtil
import com.amazon.milan.flink.types.ArrayRecord
import com.amazon.milan.flink.typeutil.TupleRecordTypeDescriptor
import com.amazon.milan.typeutil.{TupleTypeDescriptor, TypeDescriptor}


trait TypeEmitter {
  def getTypeFullName(ty: TypeDescriptor[_]): String

  def getTupleElementField(index: Int): String
}


class DefaultTypeEmitter extends TypeEmitter {
  override def getTypeFullName(ty: TypeDescriptor[_]): String = ty.fullName

  override def getTupleElementField(index: Int): String = "_" + (index + 1).toString
}


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
}
