package com.amazon.milan.application.sinks

import com.amazon.milan.application.DataSink
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

/**
 * A data sink that writes records to an SQS queue.
 *
 * @param sinkId   The unique ID of the sink in the application.
 * @param queueUrl The URL of the queue to write to, or None if the URL will be supplied in a different way.
 *                 If the URL isn't specified here, it will need to be supplied when the generated code is invoked.
 * @tparam T The type of objects accepted by the data sink.
 */
@JsonSerialize
@JsonDeserialize
class SqsDataSink[T: TypeDescriptor](val sinkId: String, val queueUrl: Option[String] = None) extends DataSink[T] {
  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]

  def this(sinkId: String, queueUrl: String) {
    this(sinkId, Some(queueUrl))
  }

  override def getGenericArguments: List[TypeDescriptor[_]] = List(this.recordTypeDescriptor)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }
}
