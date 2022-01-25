package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.graph.DiGraph.Node
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.program.StreamExpression


object ApplicationPartitioner {
  /**
   * Partitions an [[ApplicationInstance]] into one or more instances, each of which contains at most one stateful
   * operation.
   *
   * @param applicationInstance The application instance to partition.
   * @return A list of [[ApplicationInstance]] objects representing the application partitions.
   */
  def partitionApplication(applicationInstance: ApplicationInstance): List[ApplicationInstance] = {
    // First we need to get a subgraph containing just the streams that have sources defined and everything downstream.
    val diGraph = applicationInstance.application.streams.toDiGraph

    def getNodeIdIfStateful(node: Node): Option[String] =
      if (node.expr.stateful) Some(node.nodeId) else None

    val partitions = diGraph.partition("", getNodeIdIfStateful)

    partitions.map { case (key, partition) =>
      val partitionAppId = s"${applicationInstance.application.applicationId}-$key"
      val partitionStreams = StreamCollection.build(partition.nodes.map(_.expr).toList)
      val partitionApplication = new Application(partitionAppId, partitionStreams, applicationInstance.application.version)
      val partitionConfig = this.partitionConfiguration(applicationInstance.config, partitionStreams.getDereferencedStreams)
      new ApplicationInstance(partitionAppId, partitionApplication, partitionConfig)
    }.toList
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
