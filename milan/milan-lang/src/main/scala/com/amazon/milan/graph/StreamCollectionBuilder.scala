package com.amazon.milan.graph

import com.amazon.milan.lang.Stream
import com.amazon.milan.program.{Ref, SingleInputStreamExpression, StreamExpression, Tree, TwoInputStreamExpression}
import com.amazon.milan.typeutil.DataStreamTypeDescriptor


class StreamCollectionBuilder {
  private var streams: Map[String, StreamExpression] = Map.empty

  /**
   * Gets a [[StreamCollection]] containing all streams that have been added to the builder.
   *
   * @return A [[StreamCollection]] object.
   */
  def build(): StreamCollection = {
    new StreamCollection(streams.values.toList)
  }

  /**
   * Adds a stream and any upstream dependencies to the graph.
   *
   * @param stream The stream to add.
   */
  def addStream(stream: Stream[_]): StreamCollectionBuilder = {
    this.addWithReferences(stream.expr)
    this
  }

  /**
   * Adds a stream expression and any upstream dependencies to the graph.
   *
   * @param expr The stream expression to add.
   */
  def addStream(expr: StreamExpression): StreamCollectionBuilder = {
    this.addWithReferences(expr)
    this
  }

  /**
   * Recursively adds a [[StreamExpression]] object to the graph, replacing input streams with references.
   *
   * @param stream A [[StreamExpression]] object to add to the graph.
   */
  private def addWithReferences(stream: StreamExpression): Unit = {
    if (this.streams.contains(stream.nodeId)) {
      return
    }

    // First add any child streams as separate entries in the database.
    this.getInputStreams(stream).foreach(this.addWithReferences)

    // Get a version of the node with child nodes replaced with references.
    // This prevents us from deep-copying any nodes.
    val exprWithReferences = this.replaceChildStreamsWithReferences(stream)

    if (this.isDataStream(stream)) {
      this.streams = this.streams + (stream.nodeId -> exprWithReferences)
    }
  }

  /**
   * Gets any stream inputs to stream expressions.
   */
  private def getInputStreams(stream: StreamExpression): List[StreamExpression] = {
    def streamOrEmpty(expr: Tree): List[StreamExpression] = expr match {
      case s: StreamExpression => List(s)
      case _ => List()
    }

    stream match {
      case SingleInputStreamExpression(input) => streamOrEmpty(input)
      case TwoInputStreamExpression(left, right) => streamOrEmpty(left) ++ streamOrEmpty(right)
      case _ => List()
    }
  }

  /**
   * Gets a copy of an expression tree with any input [[StreamExpression]] nodes replaced with [[Ref]]
   * nodes.
   *
   * @param stream A straem expression.
   * @return A copy of the expression tree with graph nodes replaced with references.
   */
  private def replaceChildStreamsWithReferences(stream: StreamExpression): StreamExpression = {
    val refs = this.getInputStreams(stream).map(this.replaceStreamsWithReferences)
    val newChildren = refs ++ stream.getChildren.toList.drop(refs.length)
    stream.replaceChildren(newChildren).asInstanceOf[StreamExpression]
  }

  /**
   * Gets a copy of an expression tree with [[StreamExpression]] nodes replaced with [[Ref]] nodes.
   *
   * @param stream A stream expression tree.
   * @return A copy of the expression tree with stream expressions replaced with references.
   */
  private def replaceStreamsWithReferences(stream: StreamExpression): StreamExpression = {
    if (this.isDataStream(stream)) {
      new Ref(stream.nodeId, stream.nodeName, stream.tpe)
    }
    else {
      this.replaceChildStreamsWithReferences(stream)
    }
  }

  /**
   * Gets whether a stream expression represents a data stream (as opposed to a grouped, joined, or some other logical
   * stream).
   */
  private def isDataStream(stream: StreamExpression): Boolean =
    stream.tpe.isInstanceOf[DataStreamTypeDescriptor]
}