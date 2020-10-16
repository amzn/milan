package com.amazon.milan.graph

import com.amazon.milan.program.StreamExpression
import com.amazon.milan.typeutil.TypeDescriptor


object FlowGraph {

  case class Node(expr: StreamExpression,
                  contextStream: Option[StreamExpression],
                  contextKeyType: Option[TypeDescriptor[_]],
                  children: List[Node]) {
    def nodeId: String = this.expr.nodeId
  }

  /**
   * Gets a [[FlowGraph]] where the specified streams are the outputs.
   */
  def build(outputStreams: Iterable[StreamExpression]): FlowGraph = {
    val builder = new FlowGraphBuilder
    outputStreams.foreach(builder.addStream)
    builder.build
  }

  /**
   * Gets an empty [[FlowGraphBuilder]] instance.
   *
   * @return
   */
  def builder = new FlowGraphBuilder
}

import com.amazon.milan.graph.FlowGraph._


/**
 * A view of a graph of Milan stream expressions where data "flows" from parent to child nodes.
 * This is an inverted view of the graph from the one provided by [[DependencyGraph]].
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
}
