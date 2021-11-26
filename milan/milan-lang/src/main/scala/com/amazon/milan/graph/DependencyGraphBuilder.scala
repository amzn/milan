package com.amazon.milan.graph

import com.amazon.milan.graph.DependencyGraph.Node
import com.amazon.milan.lang.Stream
import com.amazon.milan.program.StreamExpression
import com.amazon.milan.typeutil.TypeDescriptor


object DependencyGraphBuilder {

  class NodeBuilder(stream: StreamExpression,
                    contextStream: Option[StreamExpression],
                    contextKeyType: Option[TypeDescriptor[_]]) {
    private var children: List[NodeBuilder] = List.empty

    def addChild(child: NodeBuilder): Unit = {
      this.children = this.children :+ child
    }

    def getChildren: Iterable[NodeBuilder] = {
      this.children
    }

    def build(nodes: Map[String, Node]): Node = {
      val builtChildren = this.children.map(child => nodes(child.nodeId))
      DependencyGraph.Node(this.stream, this.contextStream, this.contextKeyType, builtChildren)
    }

    override def toString: String = s"NodeBuilder(${this.nodeId})"

    def nodeId: String = stream.nodeId

    override def equals(obj: Any): Boolean = obj match {
      case o: NodeBuilder => this.nodeId == o.nodeId
      case _ => false
    }

    override def hashCode(): Int = this.nodeId.hashCode
  }

}

import com.amazon.milan.graph.DependencyGraphBuilder.NodeBuilder


class DependencyGraphBuilder extends GraphTraverser[NodeBuilder] {
  private var rootNodes: Set[NodeBuilder] = Set.empty

  /**
   * Gets the [[DependencyGraph]] containing the expressions that have been added to the builder.
   */
  def build: DependencyGraph = {
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
    new DependencyGraph(builtRootNodes)
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
    this.traverse(expr)
    this
  }

  override protected def registerDependency(parent: NodeBuilder, child: NodeBuilder): Unit = {
    parent.addChild(child)

    // Make sure we no longer think the child node is a root node.
    this.rootNodes = this.rootNodes - child
  }

  override protected def createNode(expr: StreamExpression, context: Context): NodeBuilder = {
    val node = new NodeBuilder(expr, context.contextStream, context.contextKeyType)

    // Until this node gets added as a child, we'll assume it's a root node.
    this.rootNodes = this.rootNodes + node

    node
  }

}
