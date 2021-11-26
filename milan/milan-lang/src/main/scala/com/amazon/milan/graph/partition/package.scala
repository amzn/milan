package com.amazon.milan.graph

import com.amazon.milan.program.{ExternalStream, StreamExpression}


package object partition {
  /**
   * Partitions a stream graph into one or more subgraphs using a user-supplied partition function.
   *
   * @param streams   A stream graph.
   * @param labelNode A function that takes the current partition and a stream expressions and returns the partition
   *                  label to assign to the expression. For root nodes the current partition will be None.
   * @tparam TPartition The type of partition labels.
   * @return A list of [[StreamCollection]] objects containing the subgraphs.
   */
  def partitionGraph[TPartition](streams: StreamCollection,
                                 labelNode: (Option[TPartition], StreamExpression) => TPartition): List[StreamCollection] = {
    val dependencyGraph = DependencyGraph.build(streams.getDereferencedStreams)

    // Assign all nodes in the input StreamCollection to partitions.
    val (finalLabels, finalExpressions) =
      dependencyGraph.rootNodes.foldLeft((Map.empty[String, TPartition], Map.empty[String, StreamExpression]))((foldingState, node) => {
        val (labels, expressions) = foldingState

        val (newLabels, newExpressions, _) = this.partitionImpl(node.expr, None, labels, expressions, labelNode)
        (newLabels, newExpressions)
      })

    // Now we want to create a StreamCollection for each partition containing all of the nodes for that partition.
    val graphs = finalExpressions
      .map {
        case (streamId, streamExpr) =>
          (finalLabels(streamId), streamExpr)
      }
      .groupBy { case (label, _) => label }
      .map { case (label, items) => label -> items.values.toList }
      .map { case (label, items) => label -> StreamCollection.build(items) }
      .values
      .toList

    graphs
  }

  private def partitionImpl[TPartition](stream: StreamExpression,
                                        currentPartition: Option[TPartition],
                                        labels: Map[String, TPartition],
                                        expressions: Map[String, StreamExpression],
                                        labelNode: (Option[TPartition], StreamExpression) => TPartition): (Map[String, TPartition], Map[String, StreamExpression], StreamExpression) = {
    // When we find a new partition, we need to break that edge by replacing the input with an ExternalStream expression.

    // Get the label for this expression.
    val streamLabel = labelNode(currentPartition, stream)

    // Recursively partition the input streams, building up the label and stream maps along the way.
    val scanResult =
      getInputStreams(stream)
        .scanLeft((labels + (stream.nodeId -> streamLabel), expressions, Option.empty[StreamExpression]))((foldingState: (Map[String, TPartition], Map[String, StreamExpression], Option[StreamExpression]), inputExpr: StreamExpression) => {
          val (foldingLabels, foldingExpressions, _) = foldingState
          val (outputLabels, outputStreams, outputExpr) = partitionImpl(inputExpr, Some(streamLabel), foldingLabels, foldingExpressions, labelNode)
          (outputLabels, outputStreams, Some(outputExpr))
        })

    // Replace the input streams with the new versions.
    val newInputs = scanResult.map { case (_, _, newInput) => newInput }.filter(_.isDefined).map(_.get)
    val streamWithNewInputs = replaceInputStreams(stream, newInputs)

    val (scanLabels, scanExpressions, _) = scanResult.last

    val labelsWithStream = scanLabels + (stream.nodeId -> streamLabel)
    val expressionsWithStream = scanExpressions + (stream.nodeId -> stream)

    currentPartition match {
      case Some(otherLabel) if !streamLabel.equals(otherLabel) =>
        // The label for this node is different from the parent.
        // We need to replace this node in the parent with an ExternalStream.
        val replacement = new ExternalStream(stream.nodeId + "_ext", stream.nodeName, stream.streamType)

        val labelsWithReplacement = labelsWithStream + (replacement.nodeId -> otherLabel)
        val expressionsWithReplacement = expressionsWithStream + (replacement.nodeId -> replacement)
        (labelsWithReplacement, expressionsWithReplacement, replacement)

      case _ =>
        (labelsWithStream, expressionsWithStream, streamWithNewInputs)
    }
  }
}
