package com.amazon.milan.application.sources

import com.amazon.milan.application.DataSource
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

/**
 * A [[DataSource]] for reading items from a DynamoDb stream.
 *
 * @param tableName The name of a DynamoDb table, or None if the table name will be supplied in a different way.
 *                  If the table name isn't specified here, it will need to be supplied when the generated code is invoked.
 * @tparam T The type of objects produced by the data source.
 */
@JsonSerialize
@JsonDeserialize
class DynamoDbStreamSource[T: TypeDescriptor](val tableName: Option[String] = None)
  extends DataSource[T] {

  def this(tableName: String) {
    this(Some(tableName))
  }

  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]

  override def getGenericArguments: List[TypeDescriptor[_]] = List(this.recordTypeDescriptor)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }
}
