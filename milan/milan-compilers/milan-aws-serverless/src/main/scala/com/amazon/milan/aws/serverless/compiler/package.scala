package com.amazon.milan.aws.serverless

import com.amazon.milan.application.DataSource
import com.amazon.milan.application.sinks.SqsDataSink
import com.amazon.milan.application.sources.SqsDataSource
import com.amazon.milan.aws.serverless.application.StreamPointerDataSource
import com.amazon.milan.typeutil.TypeDescriptor

package object compiler {
  /**
   * If a data source is a [[StreamPointerDataSource]], gets a concrete version of the source by following the pointer
   * to the data sink and constructing a [[DataSource]] that matches the sink.
   *
   * @param source A data source that may be a [[StreamPointerDataSource]]
   * @return A version of the data source that is not a pointer.
   */
  def toConcreteDataSource(source: DataSource[_]): DataSource[_] = {
    source match {
      case pointer: StreamPointerDataSource[_] =>
        val recordType = source.getGenericArguments.head.asInstanceOf[TypeDescriptor[Any]]
        pointer.sourceSink match {
          case sqs: SqsDataSink[_] =>
            new SqsDataSource[Any](sqs.queueUrl)(recordType)
        }

      case _ =>
        source
    }
  }
}
