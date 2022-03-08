package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.graph.DiGraph.Node
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.program.{ExternalStream, StreamExpression}


/**
 * A reference to a stream in an application instance.
 */
case class StreamReference(applicationInstance: ApplicationInstance, streamId: String) {
  def getStream: StreamExpression = {
    this.applicationInstance.application.streams.getStream(this.streamId)
  }
}


trait PartitionerInputOutputProcessor {
  /**
   * Handle what should happen when the application partitioner splits along a stream boundary.
   *
   * @param source       The source stream.
   * @param destinations Streams that consume the source stream as an external stream.
   */
  def processNewInputOutputMappings(source: StreamReference,
                                    destinations: List[StreamReference])
}


/**
 * A [[PartitionerInputOutputProcessor]] that does nothing.
 */
class NoOpPartitionInputOutputProcessor extends PartitionerInputOutputProcessor {
  override def processNewInputOutputMappings(source: StreamReference, destinations: List[StreamReference]): Unit = {
  }
}


object ApplicationPartitioner {
  /**
   * Partitions an [[ApplicationInstance]] into one or more instances, each of which contains at most one stateful
   * operation.
   *
   * @param applicationInstance The application instance to partition.
   * @return A list of [[ApplicationInstance]] objects representing the application partitions.
   */
  def partitionApplication(applicationInstance: ApplicationInstance,
                           inputOutputProcessor: PartitionerInputOutputProcessor): List[ApplicationInstance] = {
    // First we need to get a subgraph containing just the streams that have sources defined and everything downstream.
    val diGraph = applicationInstance.application.streams.toDiGraph

    def getNodeIdIfStateful(node: Node): Option[String] =
      if (node.expr.stateful) Some(node.nodeId) else None

    val partitions = diGraph.partition("", getNodeIdIfStateful)

    val partitionInstances =
      partitions.map { case (key, partition) =>
        val partitionAppId = s"${applicationInstance.application.applicationId}-$key"
        val partitionStreams = StreamCollection.build(partition.nodes.map(_.expr).toList)
        val partitionApplication = new Application(partitionAppId, partitionStreams, applicationInstance.application.version)
        val partitionConfig = this.partitionConfiguration(applicationInstance.config, partitionStreams.getDereferencedStreams)
        new ApplicationInstance(partitionAppId, partitionApplication, partitionConfig)
      }.toList

    this.processNewInputOutputMappings(partitionInstances, inputOutputProcessor)

    partitionInstances
  }

  private def processNewInputOutputMappings(instances: List[ApplicationInstance],
                                            processor: PartitionerInputOutputProcessor): Unit = {
    def pickSourceStream(streams: List[(StreamExpression, ApplicationInstance)]): StreamReference = {
      streams.find { case (expr, _) => !expr.isInstanceOf[ExternalStream] } match {
        case Some((expr, instance)) =>
          StreamReference(instance, expr.nodeId)

        case None =>
          streams.find { case (expr, instance) => instance.config.dataSources.exists(_.streamId == expr.nodeId) } match {
            case Some((expr, instance)) =>
              StreamReference(instance, expr.nodeId)

            case None =>
              throw new IllegalArgumentException("All streams are external streams with no source.")
          }
      }
    }

    // Create a map of stream IDs to the application instances where those streams are defined.
    // We want the instance of the stream that is not an external stream.
    // If we can't find one, we choose one that has a source defined.
    val sourceStreams = instances
      .flatMap(instance => instance.application.streams.streams.map(stream => (stream, instance)))
      .groupBy { case (stream, _) => stream.nodeId }
      .map { case (streamId, streams) => streamId -> pickSourceStream(streams) }

    def getExternalStreamsWithNoSource(instance: ApplicationInstance): List[StreamReference] = {
      val config = instance.config
      instance.application.streams.streams
        .filter(stream => stream.isInstanceOf[ExternalStream] && !config.dataSources.exists(_.streamId == stream.nodeId))
        .map(stream => StreamReference(instance, stream.nodeId))
    }

    // Find all external streams that have no source in their respective configs.
    val externalStreamsWithNoSource = instances
      .flatMap(getExternalStreamsWithNoSource)
      .groupBy(_.streamId)

    externalStreamsWithNoSource.foreach { case (streamId, streams) =>
      sourceStreams.get(streamId) match {
        case Some(sourceStream) =>
          processor.processNewInputOutputMappings(sourceStream, streams)

        case None =>
          throw new IllegalArgumentException(s"Stream $streamId is an external stream with no data source.")
      }
    }
  }

  private def partitionConfiguration(config: ApplicationConfiguration,
                                     streams: Iterable[StreamExpression]): ApplicationConfiguration = {
    val streamIds = streams.map(_.nodeId).toSet

    val partitionSinks = config.dataSinks.filter(sink => streamIds.contains(sink.streamId))
    val partitionSources = config.dataSources.filter(source => streamIds.contains(source.streamId))
    val partitionStateStores = config.stateStores.filter(store => streamIds.contains(store.operationId))

    new ApplicationConfiguration(partitionSources, partitionSinks, config.lineageSinks, config.metrics, partitionStateStores)
  }
}
