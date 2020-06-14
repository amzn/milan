package com.amazon.milan.compiler.flink.runtime

import java.io.OutputStream

import com.amazon.milan.dataformats.DataOutputFormat
import com.amazon.milan.compiler.flink.types.ArrayRecord
import com.amazon.milan.typeutil.TypeDescriptor

abstract class TupleDataOutputFormatWrapper[TTuple](innerDataOutputFormat: DataOutputFormat[TTuple])
  extends DataOutputFormat[ArrayRecord] {

  protected def getTuple(record: ArrayRecord): TTuple

  override def writeValue(value: ArrayRecord, outputStream: OutputStream): Unit =
    innerDataOutputFormat.writeValue(this.getTuple(value), outputStream)

  override def writeValues(values: TraversableOnce[ArrayRecord], outputStream: OutputStream): Unit =
    innerDataOutputFormat.writeValues(values.map(this.getTuple), outputStream)

  override def getGenericArguments: List[TypeDescriptor[_]] = List()

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = ()
}
