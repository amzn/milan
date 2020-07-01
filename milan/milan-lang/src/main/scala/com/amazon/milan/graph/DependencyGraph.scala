package com.amazon.milan.graph

import com.amazon.milan.lang.Stream
import com.amazon.milan.program.{ExternalStream, StreamExpression}


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
   * Gets an empty [[DependencyGraphBuilder]] instance.
   */
  def builder = new DependencyGraphBuilder

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

    val transformedOutputs = outputs.map(output => transformGraph(output, replaceInputs))

    val builder = DependencyGraph.builder
    transformedOutputs.foreach(output => builder.addStream(output))

    builder.build
  }
}

/**
 * A view of a graph of Milan stream expressions where parent nodes depend on their child nodes.
 * This is an inverted view of the graph from the one provided by [[FlowGraph]].
 */
class DependencyGraph(val rootNodes: List[StreamExpression]) {
  /**
   * Gets a list of the expressions in the graph in topological order.
   * A given expression in the list can depend only on other expressions that appear before it in the list.
   */
  def topologicalSort: List[StreamExpression] = {
    val (sorted, _) = this.topoSort(this.rootNodes, Set.empty[String])
    sorted
  }

  /**
   * Perform a topological sort starting at the specified node.
   *
   * @param expr    The expression to start sorting from.
   * @param visited A set of nodes that have already been visited during this sort.
   * @return A list of stream expressions in topological order.
   */
  private def topoSort(expr: StreamExpression, visited: Set[String]): (List[StreamExpression], Set[String]) = {
    expr match {
      case streamExpr: StreamExpression if visited.contains(streamExpr.nodeId) =>
        // We've already visited this expression so don't do it again.
        (List.empty, visited)

      case _ =>
        // Topo sort the input streams to this stream, then append this stream to the end since all of its
        // dependencies have been fulfilled.
        val inputStreams = getInputStreams(expr)
        val (childSorted, childVisited) = this.topoSort(inputStreams, visited)
        (childSorted :+ expr, childVisited + expr.nodeId)
    }
  }

  /**
   * Perform a topological sort starting at the specified nodes.
   *
   * @param streams The expressions to start sorting from.
   * @param visited A set of nodes that have already been visited during this sort.
   * @return A list of stream expressions in topological order.
   */
  private def topoSort(streams: Iterable[StreamExpression], visited: Set[String]): (List[StreamExpression], Set[String]) = {
    // Call topoSort for each of the streams.
    // We collect the output by concatenating the output for a node to the end of the output so far.
    // Along the way we collect the visited node IDs so that we don't duplicate any streams in the output.
    streams.foldLeft((List.empty[StreamExpression], visited))((state, child) => {
      val (stateSorted, stateVisited) = state
      val (childSorted, childVisited) = this.topoSort(child, stateVisited)
      (stateSorted ++ childSorted, childVisited)
    })
  }
}


class DependencyGraphBuilder {
  private var nodes: Map[String, StreamExpression] = Map.empty

  private var rootNodes: Set[StreamExpression] = Set.empty

  /**
   * Gets the [[DependencyGraph]] containing the expressions that have been added to the builder.
   */
  def build: DependencyGraph = {
    new DependencyGraph(this.rootNodes.toList)
  }

  /**
   * Adds a stream and its dependencies to the graph.
   *
   * @param stream A stream.
   * @return This [[DependencyGraphBuilder]] instance.
   */
  def addStream(stream: Stream[_]): DependencyGraphBuilder = {
    this.addStream(stream.expr)
  }

  /**
   * Adds a stream expression and its dependencies to the graph.
   *
   * @param expr A stream expression.
   * @return This [[DependencyGraphBuilder]] instance.
   */
  def addStream(expr: StreamExpression): DependencyGraphBuilder = {
    this.recursiveAddStream(expr, isRoot = true)
    this
  }

  private def recursiveAddStream(expr: StreamExpression,
                                 isRoot: Boolean): Unit = {
    this.nodes.get(expr.nodeId) match {
      case None =>
        if (isDataStream(expr)) {
          this.nodes = this.nodes + (expr.nodeId -> expr)

          // If this is supposed to be a root node then add it to the root nodes collection.
          if (isRoot) {
            this.rootNodes = this.rootNodes + expr
          }
        }

        getInputStreams(expr).foreach(child => this.recursiveAddStream(child, isRoot = false))

      case Some(node) =>
        // We've processed this node before so don't do anything now, except that
        // if this is not a root node then make sure it's not in the root nodes collection.
        if (!isRoot && this.rootNodes.contains(node)) {
          this.rootNodes = this.rootNodes - node
        }
    }
  }
}
