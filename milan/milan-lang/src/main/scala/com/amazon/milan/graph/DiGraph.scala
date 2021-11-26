package com.amazon.milan.graph

import com.amazon.milan.program.{ExternalStream, JoinExpression, ScanExpression, StreamExpression, Tree}
import com.amazon.milan.typeutil.TypeDescriptor

object DiGraph {

  /**
   * Gets a [[DiGraph]] containing the specified streams and any upstream dependencies.
   */
  def build(streams: StreamExpression*): DiGraph = {
    this.build(streams)
  }

  /**
   * Gets a [[DiGraph]] containing the specified streams and any upstream dependencies.
   */
  def build(streams: Iterable[StreamExpression]): DiGraph = {
    this.builder.addStreams(streams).build()
  }

  /**
   * Gets an empty [[FlowGraphBuilder]] instance.
   *
   * @return
   */
  def builder = new DiGraphBuilder

  case class Node(expr: StreamExpression, contextKeyType: Option[TypeDescriptor[_]]) {
    def isStatefulOperation: Boolean = {
      this.expr match {
        case _: JoinExpression | _: ScanExpression =>
          true

        case _ =>
          false
      }
    }

    override def hashCode(): Int = this.nodeId.hashCode

    override def equals(obj: Any): Boolean = obj match {
      case o: Node => this.nodeId == o.nodeId
      case _ => false
    }

    def nodeId: String =
      this.expr.nodeId
  }

  case class Edge(from: Node, to: Node)
}

import com.amazon.milan.graph.DiGraph._


/**
 * Encodes a directed graph of stream operations.
 *
 * @param nodes The graph nodes, representing stream operations.
 * @param edges Edges representing data flow between operations.
 */
class DiGraph(val nodes: Set[Node],
              val edges: List[Edge]) {
  /**
   * The root nodes of the graph, which have no inputs.
   */
  val rootNodes: Set[Node] = {
    val nonRootNodes = this.edges.map(_.to).toSet
    this.nodes.diff(nonRootNodes)
  }
  /**
   * The leaf nodes of the graph, which are not consumed by any other nodes.
   */
  val leafNodes: Set[Node] = {
    val nonLeafNodes = this.edges.map(_.from).toSet
    this.nodes.diff(nonLeafNodes)
  }
  /**
   * For each node, contains the edges that lead out of that node.
   */
  private val fromEdges = this.edges.groupBy(_.from.nodeId)
  /**
   * For each node, contains the edges that lead in to that node.
   */
  private val toEdges = this.edges.groupBy(_.to.nodeId)
  /**
   * Maps node IDs to nodes.
   */
  private val nodesById = this.nodes.map(node => node.nodeId -> node).toMap

  /**
   * Partitions the graph into one or more graphs based on a labelling function.
   * Each node in the input graph will appear in exactly one output graph.
   * Nodes with different labels are guaranteed to be in different output graphs.
   * When an edge is broken to create a partition, a new ExternalStream node is created to replace the input to the
   * destination node.
   *
   * @param getPartition A function that provides the partition for a node, or None if the node can be placed in any
   *                     partition.
   * @tparam T The partition type.
   * @return A map of partitions to [[DiGraph]] instances containing the corresponding graph partitions.
   */
  def partition[T](defaultPartition: T, getPartition: Node => Option[T]): Map[T, DiGraph] = {
    // Collect partition assignments for nodes that have definite assignments.
    val initialPartitionAssignments =
      this.nodes
        .map(node => (node, getPartition(node)))
        .map { case (node, partition) => partition.map(p => node -> p) }
        .filter(_.isDefined)
        .map(_.get)
        .toMap

    // If we only found a single partition then return that now.
    if (initialPartitionAssignments.size <= 1) {
      Map(defaultPartition -> this)
    }
    else {
      partition(initialPartitionAssignments, getPartition)
    }
  }

  /**
   * Partitions the graph based on initial partition assignments.
   */
  private def partition[T](initialAssignments: Map[Node, T], getPartition: Node => Option[T]): Map[T, DiGraph] = {
    // Traverse the graph to fill in partition assignments for nodes where the partitioning function didn't give us a
    // partition.
    val partitionAssignments = initialAssignments
      .foldLeft(initialAssignments)((assignments, nodeAndPartition) => {
        val (node, partition) = nodeAndPartition
        this.getTraversal(node, assignments.keySet - node)
          .foldLeft(assignments)((assignmentsSoFar, traversalNode) => assignmentsSoFar + (traversalNode -> partition))
      })

    val expressionPartitionAssignments = partitionAssignments.map { case (node, partition) => node.nodeId -> partition }

    // Now that we have the assignments we have to create new graphs where the expressions in those graphs
    // don't reference expressions outside the graphs.
    val partitions = partitionAssignments.values.toSet

    // Get the subgraph for each partition.
    // This will allow us to identify the leaf nodes, which we will use to rewrite the stream expressions replacing
    // those that reference streams in other partitions with ExternalStreams.
    val graphsPerPartition =
    partitions.toList.map(partition => {
      val partitionNodes = partitionAssignments.filter { case (node, p) => p == partition }.map { case (node, _) => node }
      partition -> this.getSubGraph(partitionNodes)
    })

    // This method transforms an expression tree, replacing any stream expressions that are in a different partition
    // with ExternalStreams.
    def transform(expr: Tree, partition: T, transformed: Map[Tree, Tree]): (Tree, Map[Tree, Tree]) = {
      if (transformed.contains(expr)) {
        (transformed(expr), transformed)
      }

      expr match {
        case streamExpr: StreamExpression if expressionPartitionAssignments(streamExpr.nodeId) != partition =>
          // This input is from another partition, so we need to replace it with an ExternalStream.
          val transformedExpr = new ExternalStream(streamExpr.nodeId, streamExpr.nodeName, streamExpr.streamType)
          (transformedExpr, transformed + (streamExpr -> transformedExpr))

        case _ =>
          val newChildren =
            expr.getChildren.scanLeft((transformed, Option.empty[Tree]))((state, childExpr) => {
              val (transformedSoFar, _) = state
              val (transformedChild, transformedWithChild) = transform(childExpr, partition, transformedSoFar)
              (transformedWithChild, Some(transformedChild))
            })
              .map { case (_, newChild) => newChild }
              .filter(_.isDefined)
              .map(_.get)
              .toList

          val transformedExpr = expr.replaceChildren(newChildren)
          (transformedExpr, transformed + (expr -> transformedExpr))
      }
    }

    def transformGraph(partition: T, graph: DiGraph): DiGraph = {
      val scanOutput = graph.leafNodes.scanLeft((Option.empty[StreamExpression], Map.empty[Tree, Tree]))((foldingState, leafNode) => {
        val (_, transformed) = foldingState
        val (newLeafExpr, newTransformed) = transform(leafNode.expr, partition, transformed)
        (Some(newLeafExpr.asInstanceOf[StreamExpression]), newTransformed)
      })

      val newLeafExprs = scanOutput.map { case (expr, _) => expr }.filter(_.isDefined).map(_.get)
      DiGraph.build(newLeafExprs)
    }

    graphsPerPartition.map { case (partition, graph) => partition -> transformGraph(partition, graph) }.toMap
  }

  /**
   * Gets a [[DiGraph]] containing subset of this graph made up of the specified nodes and any edges that connect them.
   */
  private def getSubGraph(subgraphNodes: Iterable[Node]): DiGraph = {
    val subgraphNodeSet = subgraphNodes.toSet
    val subgraphEdges = edges.filter(edge => subgraphNodeSet.contains(edge.from) && subgraphNodeSet.contains(edge.to))
    new DiGraph(subgraphNodeSet, subgraphEdges)
  }

  /**
   * Gets a sequence of nodes that traverse a subgraph starting at a specified node and not including or passing through
   * a set of boundary nodes.
   *
   * @param startNode     The node to start traversal at. It will be included in the traversal.
   * @param boundaryNodes A set of nodes that will not be included in the traversal.
   * @return A sequence of nodes that traverses the specified subgraph.
   */
  private def getTraversal(startNode: Node, boundaryNodes: Set[Node]): Iterable[Node] = {
    def getTraversal(startNode: Node, visited: Set[Node]): (List[Node], Set[Node]) = {
      if (visited.contains(startNode)) {
        Iterable.empty
      }

      val visitedWithNode = visited + startNode

      val (neighborTraversal, neighborVisited) =
        this.getNeighbors(startNode)
          .filterNot(visited.contains)
          .foldLeft((List.empty[Node], visitedWithNode))((foldingState, neighborNode) => {
            val (traversalSoFar, visitedSoFar) = foldingState
            val (traversalToAppend, newVisited) = getTraversal(neighborNode, visitedSoFar)
            (traversalSoFar ++ traversalToAppend, newVisited)
          })

      // Return a traversal that starts at this node and is followed by the neighbor traversal.
      (startNode :: neighborTraversal, neighborVisited)
    }

    val (traversal, _) = getTraversal(startNode, boundaryNodes)
    traversal
  }

  private def getNeighbors(node: Node): List[Node] =
    this.getInputs(node) ++ this.getOutputs(node)

  /**
   * Gets the input nodes to a node.
   *
   * @param node The node to get the inputs of.
   * @return A list of nodes that are inputs to the specified node.
   */
  def getInputs(node: Node): List[Node] =
    this.toEdges.getOrElse(node.nodeId, List.empty).map(_.from)

  /**
   * Gets the nodes that consume the output of a node.
   *
   * @param node A node to get the outputs of.
   * @return A list of nodes that consume the output of the specified node.
   */
  def getOutputs(node: Node): List[Node] =
    this.fromEdges.getOrElse(node.nodeId, List.empty).map(_.to)
}
