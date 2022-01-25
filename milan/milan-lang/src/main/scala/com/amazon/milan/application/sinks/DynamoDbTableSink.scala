package com.amazon.milan.application.sinks

import com.amazon.milan.application.DataSink
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * A data sink that writes records to a DynamoDb table.
 *
 * @param tableName The name of the table to write to.
 * @tparam T The type of objects accepted by the data sink.
 */
@JsonSerialize
@JsonDeserialize
class DynamoDbTableSink[T: TypeDescriptor](val sinkId: String, val tableName: String) extends DataSink[T] {
  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]

  override def getGenericArguments: List[TypeDescriptor[_]] = List(this.recordTypeDescriptor)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }
}
