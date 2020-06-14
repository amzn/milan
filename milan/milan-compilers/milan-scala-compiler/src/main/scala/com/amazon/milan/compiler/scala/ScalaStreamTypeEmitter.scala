package com.amazon.milan.compiler.scala

import com.amazon.milan.typeutil.{DataStreamTypeDescriptor, GroupedStreamTypeDescriptor, JoinedStreamsTypeDescriptor, TypeDescriptor}


/**
 * A [[TypeEmitter]] that emits stream types as their Scala Stream equivalent.
 */
class ScalaStreamTypeEmitter extends DefaultTypeEmitter {
  override def getTypeFullName(ty: TypeDescriptor[_]): String = {
    ty match {
      case _: DataStreamTypeDescriptor =>
        "scala.Stream" + this.getGenericArguments(ty)

      case _: GroupedStreamTypeDescriptor =>
        throw new NotImplementedError()

      case _: JoinedStreamsTypeDescriptor =>
        throw new NotImplementedError()

      case _ =>
        super.getTypeFullName(ty)
    }
  }
}
