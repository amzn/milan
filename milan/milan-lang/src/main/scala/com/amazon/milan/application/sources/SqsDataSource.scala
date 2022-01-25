package com.amazon.milan.application.sources

import com.amazon.milan.application.DataSource
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * A [[DataSource]] for reading items from an SQS queue.
 *
 * @param queueArn The ARN of a SQS queue.
 * @tparam T The type of objects produced by the data source.
 */
@JsonSerialize
@JsonDeserialize
class SqsDataSource[T: TypeDescriptor](val queueArn: String)
  extends DataSource[T] {

  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]

  override def getGenericArguments: List[TypeDescriptor[_]] = List(this.recordTypeDescriptor)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }
}
