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
  def transformStreamAndDependencies(stream: StreamExpression, transform: StreamExpression => StreamExpression): StreamExpression = {
    val (transformedStream, _) = this.transformGraph(stream, transform, Map.empty)
    transformedStream
  }

  /**
   * Transforms a set of stream expressions by recursively mapping the expressions and their dependencies.
   *
   * @param streams   A list of stream expression.
   * @param transform A mapping function that returns new versions of stream expressions.
   * @return The input expressions mapped using the transform function.
   */
  def transformStreamsAndDependencies(streams: Iterable[StreamExpression],
                                      transform: StreamExpression => StreamExpression): Iterable[StreamExpression] = {
    val (transformedStreams, _) =
      streams.foldLeft((List.empty[StreamExpression], Map.empty[String, StreamExpression]))((state, stream) => {
        val (currentOutput, currentTransformed) = state
        val (transformedStream, newTransformed) = this.transformGraph(stream, transform, currentTransformed)
        (currentOutput :+ transformedStream, newTransformed)
      })

    transformedStreams
  }

  /**
   * Gets a copy of an expression tree with any input [[StreamExpression]] nodes replaced with [[Ref]]
   * nodes.
   *
   * @param stream A straem expression.
   * @return A copy of the expression tree with graph nodes replaced with references.
   */
  def replaceChildStreamsWithRefs(stream: StreamExpression): StreamExpression = {
    this.transformStreamAndDependencies(stream, this.replaceStreamsWithRefs)
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
   * Replaces input streams to a stream expression with new values.
   *
   * @param stream    A stream expression.
   * @param newInputs New values of the input streams to the expression.
   * @return The input stream expression with the input streams replaced.
   */
  def replaceInputStreams(stream: StreamExpression, newInputs: List[StreamExpression]): StreamExpression = {
    val newChildren = newInputs ++ stream.getChildren.toList.drop(newInputs.length)
    stream.replaceChildren(newChildren).asInstanceOf[StreamExpression]
  }

  /**
   * Gets whether a stream expression represents a data stream (as opposed to a grouped, joined, or some other logical
   * stream).
   */
  def isDataStream(stream: StreamExpression): Boolean =
    stream.tpe.isInstanceOf[DataStreamTypeDescriptor]

  /**
   * Transforms a stream expression by recursively mapping the expression and its dependencies.
   *
   * @param stream      A stream expression.
   * @param transform   A mapping function that returns new versions of stream expressions.
   * @param transformed A map of stream IDs to previously transformed versions of those streams.
   * @return The input expression mapped using the transform function, and a new map of all transformed streams.
   */
  private def transformGraph(stream: StreamExpression,
                             transform: StreamExpression => StreamExpression,
                             transformed: Map[String, StreamExpression]): (StreamExpression, Map[String, StreamExpression]) = {
    if (transformed.contains(stream.nodeId)) {
      // If we've already transformed this expression then use the previous result.
      (transformed(stream.nodeId), transformed)
    }
    else {
      // First transform the stream, then transform any children.
      val transformedStream = transform(stream)

      // To transform the expression, first transform any input streams.
      val (newInputStreams, newTransformed) =
        this.getInputStreams(transformedStream).foldLeft((List.empty[StreamExpression], transformed))((state, inputStream) => {
          val (currentOutput, currentTransformed) = state
          val (transformedStream, newTransformed) = this.transformGraph(inputStream, transform, currentTransformed)
          (currentOutput :+ transformedStream, newTransformed)
        })

      // Replace the children in the expression with the transformed versions.
      val outputStream = this.replaceInputStreams(transformedStream, newInputStreams)

      // Add the transformed stream to the cache and return the result.
      (outputStream, newTransformed + (outputStream.nodeId -> outputStream))
    }
  }

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
