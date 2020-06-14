package com.amazon.milan.compiler.scala

import com.amazon.milan.typeutil.TypeDescriptor


trait TypeEmitter {
  def getTypeFullName(ty: TypeDescriptor[_]): String

  def getTupleElementField(index: Int): String

  def getTupleClassName(elementCount: Int): String
}


class DefaultTypeEmitter extends TypeEmitter {
  override def getTypeFullName(ty: TypeDescriptor[_]): String = {
    if (ty.isTuple && ty.genericArguments.isEmpty) {
      "Product"
    }
    else {
      ty.fullName
    }
  }

  override def getTupleElementField(index: Int): String = "_" + (index + 1).toString

  override def getTupleClassName(elementCount: Int): String = {
    if (elementCount == 0) {
      "Product"
    }
    else {
      s"Tuple$elementCount"
    }
  }

  protected def getGenericArguments(ty: TypeDescriptor[_]): String = {
    if (ty.genericArguments.isEmpty) {
      ""
    }
    else {
      ty.genericArguments.map(this.getTypeFullName).mkString("[", ", ", "]")
    }
  }
}
