package com.amazon.milan.aws.serverless.application

import com.amazon.milan.application.{ApplicationInstance, DataSink, DataSource}
import com.amazon.milan.typeutil.TypeDescriptor

/**
 * A [[DataSource]] that points to a stream in another application instance.
 * This class is not designed to be serialized.
 *
 * @param sourceInstance The application instance that contains the source stream.
 * @param sourceStreamId The ID of the source stream in the source application.
 * @tparam T The type of objects produced by the data source.
 */
class StreamPointerDataSource[T: TypeDescriptor](val sourceInstance: ApplicationInstance,
                                                 val sourceStreamId: String,
                                                 val sourceSink: DataSink[T]) extends DataSource[T] {
  private lazy val recordType = implicitly[TypeDescriptor[T]]

  override def getGenericArguments: List[TypeDescriptor[_]] = List(this.recordType)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = throw new NotImplementedError()
}
