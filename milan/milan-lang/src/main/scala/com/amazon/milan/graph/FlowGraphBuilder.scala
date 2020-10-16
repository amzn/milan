package com.amazon.milan.graph

import com.amazon.milan.graph.FlowGraph.Node
import com.amazon.milan.lang.Stream
import com.amazon.milan.program.StreamExpression
import com.amazon.milan.typeutil.TypeDescriptor


object FlowGraphBuilder {

  class NodeBuilder(val expr: StreamExpression,
                    val contextStream: Option[StreamExpression],
                    val contextKeyType: Option[TypeDescriptor[_]]) {
    private var children: List[NodeBuilder] = List.empty

    def nodeId: String = this.expr.nodeId

    def getChildren: List[NodeBuilder] = this.children

    def addChild(node: NodeBuilder): Unit = {
      this.children = this.children :+ node
    }

    def build(nodes: Map[String, Node]): Node = {
      val builtChildren = this.children.map(child => nodes(child.expr.nodeId))
      Node(this.expr, this.contextStream, this.contextKeyType, builtChildren)
    }

    override def hashCode(): Int = this.nodeId.hashCode

    override def equals(obj: Any): Boolean = obj match {
      case o: NodeBuilder => this.nodeId == o.nodeId
      case _ => false
    }
  }

}

import com.amazon.milan.graph.FlowGraphBuilder._


class FlowGraphBuilder extends GraphTraverser[NodeBuilder] {
  private var nodes: Set[NodeBuilder] = Set.empty
  private var rootNodes: Set[NodeBuilder] = Set.empty

  /**
   * Gets a [[FlowGraph]] using the current state of this builder.
   */
  def build: FlowGraph = {
    var builtNodes = Map.empty[String, Node]

    def getOrBuild(nodeBuilder: NodeBuilder): Node = {
      builtNodes.get(nodeBuilder.nodeId) match {
        case Some(node) =>
          node

        case None =>
          nodeBuilder.getChildren.foreach(getOrBuild)
          val node = nodeBuilder.build(builtNodes)
          builtNodes = builtNodes + (node.nodeId -> node)
          node
      }
    }

    val builtRootNodes = this.rootNodes.toList.map(getOrBuild)
    new FlowGraph(builtRootNodes, builtNodes)
  }

  /**
   * Adds a stream and its dependencies to the graph.
   *
   * @param stream A stream.
   * @return This [[FlowGraphBuilder]] instance.
   */
  def addStream(stream: Stream[_]): FlowGraphBuilder = {
    this.addStream(stream.expr)
  }

  /**
   * Adds a stream expression and its dependencies to the graph.
   *
   * @param expr A stream expression.
   * @return This [[FlowGraphBuilder]] instance.
   */
  def addStream(expr: StreamExpression): FlowGraphBuilder = {
    this.traverse(expr)
    this
  }

  override protected def registerDependency(parent: NodeBuilder, child: NodeBuilder): Unit = {
    // The child here is a dependency of the parent, so data flows from child to parent.
    // This means we add the parent node as a child of the child node in the flow graph.
    child.addChild(parent)

    // Make sure we no longer think the parent node is a root node.
    this.rootNodes = this.rootNodes - parent
  }

  override protected def createNode(expr: StreamExpression, context: Context): NodeBuilder = {
    val node = new NodeBuilder(expr, context.contextStream, context.contextKeyType)

    // This is a root node until proven otherwise.
    this.rootNodes = this.rootNodes + node
    this.nodes = this.nodes + node

    node
  }
}
