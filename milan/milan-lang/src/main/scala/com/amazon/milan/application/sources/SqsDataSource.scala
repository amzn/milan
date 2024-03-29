package com.amazon.milan.application.sources

import com.amazon.milan.application.DataSource
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * A [[DataSource]] for reading items from an SQS queue.
 *
 * @param queueUrl The URL of a SQS queue, or None if the URL will be supplied in a different way.
 *                 If the URL isn't specified here, it will need to be supplied when the generated code is invoked.
 * @tparam T The type of objects produced by the data source.
 */
@JsonSerialize
@JsonDeserialize
class SqsDataSource[T: TypeDescriptor](val queueUrl: Option[String] = None)
  extends DataSource[T] {

  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]

  def this(queueUrl: String) {
    this(Some(queueUrl))
  }

  override def getGenericArguments: List[TypeDescriptor[_]] = List(this.recordTypeDescriptor)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }
}
