package com.amazon.milan.lang

import com.amazon.milan.program.{Ref, StreamExpression, Tree}
import com.fasterxml.jackson.annotation.{JsonCreator, JsonIgnore}

import scala.language.experimental.macros


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

    // First add any child graph nodes as separate entries in the database.
    stream.getChildren
      .flatMap(Tree.getDataStreams)
      .foreach(this.addWithReferences)

    // Get a version of the node with child nodes replaced with references.
    // This prevents us from deep-copying any nodes.
    val exprWithReferences = Tree.replaceChildStreamsWithReferences(stream).asInstanceOf[StreamExpression]

    this.streamsById = this.streamsById + (stream.nodeId -> exprWithReferences)
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

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: StreamGraph =>
        this.streamsById.equals(o.streamsById)

      case _ =>
        false
    }
  }
}
