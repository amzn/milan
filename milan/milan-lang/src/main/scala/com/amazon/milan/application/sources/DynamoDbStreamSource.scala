package com.amazon.milan.application.sources

import com.amazon.milan.application.DataSource
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

/**
 * A [[DataSource]] for reading items from a DynamoDb stream.
 *
 * @param tableName The name of a DynamoDb table.
 * @tparam T The type of objects produced by the data source.
 */
@JsonSerialize
@JsonDeserialize
class DynamoDbStreamSource[T: TypeDescriptor](val tableName: String)
  extends DataSource[T] {

  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]

  override def getGenericArguments: List[TypeDescriptor[_]] = List(this.recordTypeDescriptor)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }
}
