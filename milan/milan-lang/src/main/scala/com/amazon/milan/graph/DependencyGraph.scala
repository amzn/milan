package com.amazon.milan.graph

import com.amazon.milan.lang.Stream
import com.amazon.milan.program.{ExternalStream, StreamExpression}
import com.amazon.milan.typeutil.TypeDescriptor


object DependencyGraph {

  /**
   * Builds a [[DependencyGraph]] with a single output stream.
   */
  def build(outputStream: Stream[_]): DependencyGraph = {
    this.build(outputStream.expr)
  }

  /**
   * Builds a [[DependencyGraph]] with a single output stream.
   */
  def build(outputStream: StreamExpression): DependencyGraph = {
    this.builder.addStream(outputStream).build
  }

  /**
   * Builds a [[DependencyGraph]] that contains the specified streams.
   */
  def build(streams: Iterable[StreamExpression]): DependencyGraph = {
    val builder = new DependencyGraphBuilder
    streams.foreach(builder.addStream)
    builder.build
  }

  /**
   * Gets the minimal subgraph that terminates at the specified output nodes, and does not contain any nodes that the
   * specified input nodes depend on.
   *
   * @param inputs  The input expressions for the subgraph.
   *                In the resulting subgraph these will be replaced with ExternalStream expressions.
   * @param outputs The output expressions for the subgraph.
   * @return A [[DependencyGraph]] containing the specified subgraph.
   */
  def buildSubGraph(inputs: Iterable[StreamExpression], outputs: Iterable[StreamExpression]): DependencyGraph = {
    // In the output expressions we'll be replacing the input streams with ExternalStream expressions of the same type,
    // then building the dependency graph using those modified output streams.
    val newInputs =
    inputs
      .map(input => input.nodeId -> new ExternalStream(input.nodeId, input.nodeName, input.tpe.asStream))
      .toMap

    def replaceInputs(expr: StreamExpression): StreamExpression =
      newInputs.getOrElse(expr.nodeId, expr)

    val transformedOutputs = outputs.map(output => transformStreamAndDependencies(output, replaceInputs))

    val builder = DependencyGraph.builder
    transformedOutputs.foreach(output => builder.addStream(output))

    builder.build
  }

  /**
   * Gets an empty [[DependencyGraphBuilder]] instance.
   */
  def builder = new DependencyGraphBuilder

  /**
   * A node in a dependency graph.
   *
   * @param expr          The stream expression represented by this node.
   * @param contextStream The context of the stream.
   *                      If the stream exists as part of a function of another stream (e.g. inside the mapping function of a
   *                      Map or FlatMap) then this will reference the parent stream.
   * @param children      The nodes that this node depends on.
   */
  case class Node(expr: StreamExpression,
                  contextStream: Option[StreamExpression],
                  contextKeyType: Option[TypeDescriptor[_]],
                  children: List[Node]) {
    def nodeId: String = expr.nodeId

    override def hashCode(): Int = expr.nodeId.hashCode

    override def equals(obj: Any): Boolean = obj match {
      case o: Node => this.expr.nodeId == o.expr.nodeId
      case _ => false
    }
  }
}

import com.amazon.milan.graph.DependencyGraph.Node


/**
 * A view of a graph of Milan stream expressions where parent nodes depend on their child nodes.
 * This is an inverted view of the graph from the one provided by [[FlowGraph]].
 *
 * @param rootNodes A list of nodes that have no dependencies (i.e. no downstream consumers) in the graph.
 */
class DependencyGraph(val rootNodes: List[Node]) {
  /**
   * Gets a list of the expressions in the graph in topological order.
   * A given expression in the list can depend only on other expressions that appear before it in the list.
   */
  def topologicalSort: List[Node] = {
    val (sorted, _) = this.topoSort(this.rootNodes, Set.empty[String])
    sorted
  }

  /**
   * Perform a topological sort starting at the specified node.
   *
   * @param node    The node to start sorting from.
   * @param visited A set of nodes that have already been visited during this sort.
   * @return A list of nodes in topological order.
   */
  private def topoSort(node: Node, visited: Set[String]): (List[Node], Set[String]) = {
    if (visited.contains(node.nodeId)) {
      (List.empty, visited)
    }
    else {
      // Topo sort the input streams to this stream, then append this stream to the end since all of its
      // dependencies have been fulfilled.
      val (childSorted, childVisited) = this.topoSort(node.children, visited)
      (childSorted :+ node, childVisited + node.nodeId)
    }
  }

  /**
   * Perform a topological sort starting at the specified nodes.
   *
   * @param nodes   The nodes to start sorting from.
   * @param visited A set of nodes that have already been visited during this sort.
   * @return A list of nodes in topological order.
   */
  private def topoSort(nodes: Iterable[Node], visited: Set[String]): (List[Node], Set[String]) = {
    // Call topoSort for each of the streams.
    // We collect the output by concatenating the output for a node to the end of the output so far.
    // Along the way we collect the visited node IDs so that we don't duplicate any streams in the output.
    nodes.foldLeft((List.empty[Node], visited))((state, node) => {
      val (stateSorted, stateVisited) = state
      val (nodeSorted, nodeVisited) = this.topoSort(node, stateVisited)
      (stateSorted ++ nodeSorted, nodeVisited)
    })
  }
}
