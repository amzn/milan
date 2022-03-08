package com.amazon.milan.application.sinks

import com.amazon.milan.application.DataSink
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * A data sink that writes records to a DynamoDb table.
 *
 * @param tableName The name of the table to write to, or None if the table name will be supplied in a different way.
 *                  If the table name isn't specified here, it will need to be supplied when the generated code is invoked.
 * @tparam T The type of objects accepted by the data sink.
 */
@JsonSerialize
@JsonDeserialize
class DynamoDbTableSink[T: TypeDescriptor](val sinkId: String, val tableName: Option[String] = None) extends DataSink[T] {
  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]

  def this(sinkId: String, tableName: String) {
    this(sinkId, Some(tableName))
  }

  override def getGenericArguments: List[TypeDescriptor[_]] = List(this.recordTypeDescriptor)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }
}
