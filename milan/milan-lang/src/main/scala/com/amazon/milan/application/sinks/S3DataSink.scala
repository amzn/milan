package com.amazon.milan.application.sinks

import com.amazon.milan.application.DataSink
import com.amazon.milan.typeutil.TypeDescriptor


class S3DataSink[T: TypeDescriptor](region: String, prefix: String, keyFormat: String) extends DataSink[T] {
  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]

  override def getGenericArguments: List[TypeDescriptor[_]] = List(this.recordTypeDescriptor)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }
}
