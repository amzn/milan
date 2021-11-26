package com.amazon.milan.graph

import com.amazon.milan.graph.DiGraph._
import com.amazon.milan.lang.Stream
import com.amazon.milan.program.StreamExpression


class DiGraphBuilder extends GraphTraverser[Node] {
  private var nodes: Set[Node] = Set.empty
  private var edges: Set[Edge] = Set.empty
  private var rootNodes: Set[Node] = Set.empty
  private var leafNodes: Set[Node] = Set.empty

  def build(): DiGraph = {
    new DiGraph(this.nodes, this.edges.toList)
  }

  /**
   * Adds a stream and its dependencies to the graph.
   *
   * @param stream A stream.
   * @return This [[DiGraphBuilder]] instance.
   */
  def addStream(stream: Stream[_]): DiGraphBuilder = {
    this.addStream(stream.expr)
  }

  /**
   * Adds a stream expression and its dependencies to the graph.
   *
   * @param expr A stream expression.
   * @return This [[DiGraphBuilder]] instance.
   */
  def addStream(expr: StreamExpression): DiGraphBuilder = {
    this.traverse(expr)
    this
  }

  /**
   * Adds a set stream expressions and their dependencies to the graph.
   *
   * @param exprs A set of stream expressions.
   * @return This [[DiGraphBuilder]] instance.
   */
  def addStreams(exprs: Iterable[StreamExpression]): DiGraphBuilder = {
    exprs.foreach(this.addStream)
    this
  }

  /**
   * Adds a stream expression and its dependencies to the graph.
   * Traversal of dependencies halts when a boundary condition is true.
   *
   * @param expr                 A stream expression.
   * @param isBoundaryExpression A boundary condition that identifies expressions that should not be included in the graph.
   * @return This [[DiGraphBuilder]] instance.
   */
  def addStream(expr: StreamExpression, isBoundaryExpression: StreamExpression => Boolean): DiGraphBuilder = {
    this.traverse(expr, isBoundaryExpression)
    this
  }

  override protected def registerDependency(parent: Node, child: Node): Unit = {
    // Edges represent data flow, so the child is the source and the parent is the destination.
    this.edges = this.edges + Edge(child, parent)

    // The parent node can't be a root, and the child node can't be a leaf.
    this.rootNodes = this.rootNodes - parent
    this.leafNodes = this.leafNodes - child
  }

  override protected def createNode(expr: StreamExpression, context: Context): Node = {
    val node = Node(expr, context.contextKeyType)
    this.nodes = this.nodes + node

    // This is a root node and a left node until proven otherwise.
    this.rootNodes = this.rootNodes + node
    this.leafNodes = this.leafNodes + node

    node
  }
}
