package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.sinks.SqsDataSink
import com.amazon.milan.aws.serverless.application.StreamPointerDataSource
import com.amazon.milan.typeutil.TypeDescriptor


/**
 * Implementation of [[PartitionerInputOutputProcessor]] that connects streams using an SQS queue.
 */
class SqsInputOutputMappingProcessor extends PartitionerInputOutputProcessor {
  /**
   * Handle what should happen when the application partitioner splits along a stream boundary.
   *
   * @param source       The source stream.
   * @param destinations Streams that consume the source stream as an external stream.
   */
  override def processNewInputOutputMappings(source: StreamReference, destinations: List[StreamReference]): Unit = {
    destinations.foreach(dest => this.mapStreams(source, dest))
  }

  private def mapStreams(source: StreamReference, destination: StreamReference): Unit = {
    // Create an SQS sink for the source stream and add it to the config.
    // We don't have a type parameter for the record type, so create something generic and pass the TypeDescriptor
    // explicitly.
    val sinkId = s"${source.streamId}_GeneratedSqsSink"
    val sourceStream = source.getStream
    val sourceRecordType = sourceStream.recordType.asInstanceOf[TypeDescriptor[Any]]
    val dataSink = new SqsDataSink[Any](sinkId)(sourceRecordType)
    source.applicationInstance.config.addSink(source.streamId, dataSink)

    // Set the data source for the destination.
    // We use a StreamPointerDataSource so that the compiler can best decide how to hook them up.
    val dataSource = new StreamPointerDataSource[Any](source.applicationInstance, source.streamId, dataSink)(sourceRecordType)
    destination.applicationInstance.config.setSource(destination.streamId, dataSource)
  }
}
