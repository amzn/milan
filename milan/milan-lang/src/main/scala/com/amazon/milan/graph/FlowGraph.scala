package com.amazon.milan.graph

import com.amazon.milan.program.StreamExpression


object FlowGraph {

  case class Node(expr: StreamExpression, children: List[Node]) {
  }

  class NodeBuilder(val expr: StreamExpression) {
    private var children: List[NodeBuilder] = List.empty

    def getChildren: List[NodeBuilder] = this.children

    def addChild(node: NodeBuilder): Unit = {
      this.children = this.children :+ node
    }

    def build(nodes: Map[String, Node]): Node = {
      Node(this.expr, this.children.map(child => nodes(child.expr.nodeId)))
    }
  }

}

import com.amazon.milan.graph.FlowGraph._


/**
 * A view of a graph of Milan stream expressions where data "flows" from parent to child nodes.
 * This is an inverted view of the graph from the one provided by [[DependencyGraph]].
 */
class FlowGraph(val rootNodes: List[Node]) {

}


class FlowGraphBuilder {
  private var nodes: Map[String, NodeBuilder] = Map.empty

  private var rootNodes: List[NodeBuilder] = List.empty

  /**
   * Gets a [[FlowGraph]] using the current state of this builder.
   */
  def build: FlowGraph = {
    val builtNodes = this.rootNodes.foldLeft(Map.empty[String, Node])((nodes, node) => this.recursiveBuild(node, nodes))
    val builtRootNodes = this.rootNodes.map(node => builtNodes(node.expr.nodeId))
    new FlowGraph(builtRootNodes)
  }

  /**
   * Adds a [[StreamExpression]], and recursively any [[StreamExpression]]s that it depends on, to the graph.
   */
  def addStream(expr: StreamExpression): Unit = {
    this.recursiveAddStream(expr)
  }

  /**
   * Adds a stream and its dependencies to the graph.
   */
  private def recursiveAddStream(expr: StreamExpression): NodeBuilder = {
    // If we already have a node for this stream then we've already visited it, so we can skip it.
    this.nodes.get(expr.nodeId) match {
      case Some(node) =>
        node

      case None =>
        val node = new NodeBuilder(expr)
        this.nodes = this.nodes + (expr.nodeId -> node)

        val children = expr.getChildren.toArray

        if (children.isEmpty) {
          this.rootNodes = this.rootNodes :+ node
        }
        else {
          // This is where we "invert" the expression graph.
          // We create a node for each stream input to this stream expression, and add the node for this expression
          // as a child to each of those, because this expression depends on them.
          children
            .filter(_.isInstanceOf[StreamExpression])
            .map(_.asInstanceOf[StreamExpression])
            .foreach(childExpr => {
              val childNode = this.recursiveAddStream(childExpr)
              childNode.addChild(node)
            })
        }

        node
    }
  }

  private def recursiveBuild(node: NodeBuilder, nodes: Map[String, Node]): Map[String, Node] = {
    // TODO: finish this.
    throw new NotImplementedError()
  }
}
