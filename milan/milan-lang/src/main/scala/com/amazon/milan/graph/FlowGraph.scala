package com.amazon.milan.graph

import com.amazon.milan.program.{ExternalStream, StreamExpression}
import com.amazon.milan.typeutil.TypeDescriptor

import scala.collection.mutable


object FlowGraph {

  /**
   * Gets a [[FlowGraph]] where the specified streams are the outputs.
   */
  def build(streams: StreamExpression*): FlowGraph = {
    this.build(streams)
  }

  /**
   * Gets a [[FlowGraph]] where the specified streams are the outputs.
   */
  def build(streams: Iterable[StreamExpression]): FlowGraph = {
    this.builder.addStreams(streams).build
  }

  /**
   * Gets an empty [[FlowGraphBuilder]] instance.
   *
   * @return
   */
  def builder = new FlowGraphBuilder

  case class Node(expr: StreamExpression,
                  contextStream: Option[StreamExpression],
                  contextKeyType: Option[TypeDescriptor[_]],
                  children: List[Node]) {
    def nodeId: String = this.expr.nodeId

    def clone(nodes: mutable.HashMap[String, Node]): Node = {
      nodes.getOrElseUpdate(
        this.nodeId,
        {
          val clonedChildren = children.map(child => nodes.getOrElseUpdate(child.nodeId, child.clone(nodes)))
          Node(this.expr, this.contextStream, this.contextKeyType, clonedChildren)
        })
    }
  }
}

import com.amazon.milan.graph.FlowGraph._


/**
 * A view of a graph of Milan stream expressions where data "flows" from parent to child nodes.
 * This is an inverted view of the graph from the one provided by [[DependencyGraph]].
 *
 * @param rootNodes A list of nodes that have no inputs in the graph.
 * @param nodes     A map of stream ID to Node object for all nodes in the graph.
 */
class FlowGraph(val rootNodes: List[Node], val nodes: Map[String, Node]) {
  /**
   * Gets the stream expressions that consume the output of an expression.
   *
   * @param expr A stream expression.
   * @return A list of the stream expressions that consume the output of the specified expression.
   */
  def getDependentExpressions(expr: StreamExpression): List[StreamExpression] = {
    val node = this.nodes(expr.nodeId)
    node.children.map(_.expr)
  }

  /**
   * Gets a [[StreamCollection]] containing the streams from this graph.
   */
  def toStreamCollection: StreamCollection = {
    StreamCollection.build(this.nodes.values.map(_.expr))
  }

  /**
   * Gets a subgraph that contains the specified root nodes and any downstream nodes.
   *
   * @param subgraphRootExpressions The root expressions of the subgraph.
   * @return A [[FlowGraph]] containing only the specified subgraph.
   */
  def getSubgraph(subgraphRootExpressions: List[StreamExpression]): FlowGraph = {
    // First we identify which nodes should appear in the subgraph: all the nodes that are downstream from the root
    // expressions we were provided.
    val subgraphRootNodes = subgraphRootExpressions.map(expr => this.nodes(expr.nodeId))
    val clonedNodes = new mutable.HashMap[String, Node]()
    val clonedRootNodes = subgraphRootNodes.map(_.clone(clonedNodes))

    // Next we need to modify the stream expressions so that the root node expressions are replaced with ExternalStream.
    val rootNodeIds = subgraphRootExpressions.map(_.nodeId).toSet
    val clonedNodeIds = clonedNodes.keySet

    // This function converts any root expressions into ExternalStream expressions, we'll use it with the transformGraph
    // function to transform the entire graph.
    def transformStream(stream: StreamExpression): StreamExpression = {
      if (rootNodeIds.contains(stream.nodeId)) {
        new ExternalStream(stream.nodeId, stream.nodeName, stream.streamType)
      }
      else if (!clonedNodeIds.contains(stream.nodeId)) {
        // Check for any nodes that depend on expressions that are not in the subgraph.
        throw new IllegalArgumentException("The specified subgraph is not closed: there are input expressions that are not contained in the subgraph.")
      }
      else {
        stream
      }
    }

    // TODO: Check that all the specified root nodes are actually root nodes.

    val transformedStreams = transformStreamsAndDependencies(clonedNodes.values.map(_.expr), transformStream)

    FlowGraph.builder
      .addStreams(transformedStreams)
      .build
  }
}
