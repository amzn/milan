package com.amazon.milan

import com.amazon.milan.program.{ExternalStream, Ref, SingleInputStreamExpression, StreamExpression, Tree, TwoInputStreamExpression, TypeChecker}
import com.amazon.milan.typeutil.DataStreamTypeDescriptor


package object graph {
  /**
   * Performs type checking on a set of streams and their dependencies.
   */
  def typeCheckGraph(streams: StreamExpression*): Unit = {
    this.typeCheckGraph(streams.toList)
  }

  /**
   * Performs type checking on a set of streams and their dependencies.
   */
  def typeCheckGraph(streams: List[StreamExpression]): Unit = {
    val externalStreamTypes =
      streams.filter(_.isInstanceOf[ExternalStream]).map(_.asInstanceOf[ExternalStream])
        .map(node => node.nodeId -> node.streamType)
        .toMap

    streams.foreach(stream => TypeChecker.typeCheck(stream, externalStreamTypes))
  }

  /**
   * Transforms a stream expression by recursively mapping the expression and its dependencies.
   *
   * @param stream    A stream expression.
   * @param transform A mapping function that returns new versions of stream expressions.
   * @return The input expression mapped using the transform function.
   */
  def transformGraph(stream: StreamExpression, transform: StreamExpression => StreamExpression): StreamExpression = {
    val newInputStreams = this.getInputStreams(stream).map(transform).map(inputExpr => this.transformGraph(inputExpr, transform))
    val newChildren = newInputStreams ++ stream.getChildren.toList.drop(newInputStreams.length)
    stream.replaceChildren(newChildren).asInstanceOf[StreamExpression]
  }

  /**
   * Gets a copy of an expression tree with any input [[StreamExpression]] nodes replaced with [[Ref]]
   * nodes.
   *
   * @param stream A straem expression.
   * @return A copy of the expression tree with graph nodes replaced with references.
   */
  def replaceChildStreamsWithRefs(stream: StreamExpression): StreamExpression = {
    this.transformGraph(stream, this.replaceStreamsWithRefs)
  }

  /**
   * Gets any stream inputs to stream expressions.
   */
  def getInputStreams(stream: StreamExpression): List[StreamExpression] = {
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
   * Gets whether a stream expression represents a data stream (as opposed to a grouped, joined, or some other logical
   * stream).
   */
  def isDataStream(stream: StreamExpression): Boolean =
    stream.tpe.isInstanceOf[DataStreamTypeDescriptor]

  /**
   * Gets a copy of an expression tree with [[StreamExpression]] nodes replaced with [[Ref]] nodes.
   *
   * @param stream A stream expression tree.
   * @return A copy of the expression tree with stream expressions replaced with references.
   */
  private def replaceStreamsWithRefs(stream: StreamExpression): StreamExpression = {
    if (this.isDataStream(stream)) {
      new Ref(stream.nodeId, stream.nodeName, stream.tpe)
    }
    else {
      this.replaceChildStreamsWithRefs(stream)
    }
  }
}
