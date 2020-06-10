package com.amazon.milan.lang

import com.amazon.milan.program.{Ref, SingleInputStreamExpression, StreamExpression, Tree, TwoInputStreamExpression}
import com.amazon.milan.typeutil.DataStreamTypeDescriptor
import com.fasterxml.jackson.annotation.{JsonCreator, JsonIgnore}

import scala.language.experimental.macros


/**
 * Contains a set of streams that form a program.
 *
 * @param streamsById A map of stream IDs to the expressions that define those streams.
 */
@JsonCreator()
class StreamGraph(var streamsById: Map[String, StreamExpression]) {
  def this() {
    this(Map.empty[String, StreamExpression])
  }

  def this(streams: Stream[_]*) {
    this()
    streams.foreach(this.addStream)
  }

  /**
   * Adds a stream and any upstream dependencies to the graph.
   *
   * @param stream The stream to add.
   */
  def addStream(stream: Stream[_]): Unit = {
    this.addWithReferences(stream.expr)
  }

  /**
   * Gets the stream with the specified ID.
   *
   * @param streamId The ID of a stream.
   * @return The [[StreamExpression]] object representing the requested stream.
   */
  def getStream(streamId: String): StreamExpression = {
    this.streamsById.get(streamId) match {
      case Some(stream) =>
        stream

      case _ =>
        throw new IllegalArgumentException(s"No stream with ID '$streamId' exists.")
    }
  }

  /**
   * Gets all of the streams in the graph.
   *
   * @return An [[Iterable]] that yields the streams.
   */
  @JsonIgnore
  def getStreams: Iterable[StreamExpression] = this.streamsById.values

  /**
   * Gets the nodes in the graph with all references replaced with actual nodes where possible.
   */
  @JsonIgnore
  def getDereferencedGraph: StreamGraph = {
    val dereferencedStreams =
      streamsById.values
        .map(this.dereferenceStreamExpression)
        .map(stream => stream.nodeId -> stream)
        .toMap

    new StreamGraph(dereferencedStreams)
  }

  /**
   * Recursively adds a [[StreamExpression]] object to the graph, replacing input streams with references.
   *
   * @param stream A [[StreamExpression]] object to add to the graph.
   */
  private def addWithReferences(stream: StreamExpression): Unit = {
    if (this.streamsById.contains(stream.nodeId)) {
      return
    }

    // First add any child streams as separate entries in the database.
    this.getInputStreams(stream).foreach(this.addWithReferences)

    // Get a version of the node with child nodes replaced with references.
    // This prevents us from deep-copying any nodes.
    val exprWithReferences = this.replaceChildStreamsWithReferences(stream)

    if (this.isDataStream(stream)) {
      this.streamsById = this.streamsById + (stream.nodeId -> exprWithReferences)
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
   * Reconstructs an expression tree so that any [[Ref]] nodes are replaced with the actual nodes
   * they refer to. Any references to external streams will remain as [[Ref]] nodes.
   *
   * @param tree An expression tree.
   * @return A copy of the tree with all non-external references replaced with actual nodes.
   */
  private def dereferenceExpressionTree(tree: Tree): Tree = {
    tree match {
      case Ref(nodeId) =>
        streamsById.get(nodeId) match {
          case Some(Ref(derefNodeId)) =>
            streamsById(derefNodeId)

          case Some(expr) =>
            this.dereferenceExpressionTree(expr)

          case None =>
            tree
        }

      case _ =>
        val newChildren = tree.getChildren.map(this.dereferenceExpressionTree).toList
        tree.replaceChildren(newChildren)
    }
  }

  /**
   * Reconstructs an expression tree so that any [[Ref]] nodes are replaced with the actual nodes
   * they refer to. Any references to external streams will remain as [[Ref]] nodes.
   *
   * @param expr A [[StreamExpression]] expression tree.
   * @return A copy of the tree with all non-external references replaced with actual nodes.
   */
  private def dereferenceStreamExpression(expr: StreamExpression): StreamExpression = {
    this.dereferenceExpressionTree(expr).asInstanceOf[StreamExpression]
  }

  /**
   * Gets a copy of an expression tree with any input [[StreamExpression]] nodes replaced with [[Ref]]
   * nodes.
   *
   * @param stream A straem expression.
   * @return A copy of the expression tree with graph nodes replaced with references.
   */
  def replaceChildStreamsWithReferences(stream: StreamExpression): StreamExpression = {
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
      Ref(stream.nodeId)
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

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: StreamGraph =>
        this.streamsById.equals(o.streamsById)

      case _ =>
        false
    }
  }
}
