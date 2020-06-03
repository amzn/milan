package com.amazon.milan.flink.internal

import com.amazon.milan.lang.StreamGraph
import com.amazon.milan.program.{ExternalStream, TypeChecker}

object GraphTypeChecker {
  /**
   * Performs type checking on an entire graph.
   */
  def typeCheckGraph(graph: StreamGraph): Unit = {
    val inputNodeTypes =
      graph.getStreams.filter(_.isInstanceOf[ExternalStream]).map(_.asInstanceOf[ExternalStream])
        .map(node => node.nodeId -> node.streamType)
        .toMap

    graph.getStreams.foreach(node => TypeChecker.typeCheck(node, inputNodeTypes))
  }
}
