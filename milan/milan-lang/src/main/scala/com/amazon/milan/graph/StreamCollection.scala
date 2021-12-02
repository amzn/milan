package com.amazon.milan.graph

import com.amazon.milan.lang.Stream
import com.amazon.milan.program.{Ref, StreamExpression, Tree}
import com.fasterxml.jackson.annotation.JsonIgnore


object StreamCollection {
  def build(stream: Stream[_]): StreamCollection = {
    StreamCollection.builder.addStream(stream).build()
  }

  def build(streams: Stream[_]*): StreamCollection = {
    streams.foldLeft(StreamCollection.builder)((builder, stream) => builder.addStream(stream)).build()
  }

  def build(streamExpression: StreamExpression): StreamCollection = {
    StreamCollection.builder.addStream(streamExpression).build()
  }

  def builder: StreamCollectionBuilder = new StreamCollectionBuilder

  def build(streamExpressions: Iterable[StreamExpression]): StreamCollection = {
    streamExpressions.foldLeft(StreamCollection.builder)((builder, stream) => builder.addStream(stream)).build()
  }
}


/**
 *
 * @param streams All of the streams in the collection.
 */
class StreamCollection(val streams: List[StreamExpression]) {
  /**
   * Gets the stream with the specified ID.
   *
   * @param streamId The ID of a stream.
   * @return The [[StreamExpression]] object representing the requested stream.
   */
  def getStream(streamId: String): StreamExpression = {
    this.streams.find(_.nodeId == streamId) match {
      case Some(stream) =>
        stream

      case _ =>
        throw new IllegalArgumentException(s"No stream with ID '$streamId' exists.")
    }
  }

  /**
   * Gets a stream with the specified ID, restoring the original graph structure.
   */
  @JsonIgnore
  def getDereferencedStream(streamId: String): StreamExpression = {
    val stream = this.getStream(streamId)
    val streamsById = this.streams.map(stream => stream.nodeId -> stream).toMap
    this.dereferenceStreamExpression(streamsById)(stream)
  }

  /**
   * Gets a [[DiGraph]] containing the streams in the collection.
   */
  def toDiGraph: DiGraph = {
    val dereferencedStreams = this.getDereferencedStreams
    DiGraph.build(dereferencedStreams)
  }

  /**
   * Gets the streams in the collection, restoring the original graph structure.
   */
  @JsonIgnore
  def getDereferencedStreams: List[StreamExpression] = {
    val streamsById = this.streams.map(stream => stream.nodeId -> stream).toMap
    this.streams.map(this.dereferenceStreamExpression(streamsById))
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: StreamCollection =>
        this.streams.sortBy(_.nodeId).equals(o.streams.sortBy(_.nodeId))

      case _ =>
        false
    }
  }

  /**
   * Reconstructs an expression tree so that any [[Ref]] nodes are replaced with the actual nodes
   * they refer to. Any references to external streams will remain as [[Ref]] nodes.
   *
   * @param tree An expression tree.
   * @return A copy of the tree with all non-external references replaced with actual nodes.
   */
  private def dereferenceExpressionTree(streamsById: Map[String, StreamExpression])(tree: Tree): Tree = {
    tree match {
      case Ref(nodeId) =>
        streamsById.get(nodeId) match {
          case Some(Ref(derefNodeId)) =>
            streamsById(derefNodeId)

          case Some(expr) =>
            this.dereferenceExpressionTree(streamsById)(expr)

          case None =>
            tree
        }

      case _ =>
        val newChildren = tree.getChildren.map(this.dereferenceExpressionTree(streamsById)).toList
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
  private def dereferenceStreamExpression(streamsById: Map[String, StreamExpression])(expr: StreamExpression): StreamExpression = {
    this.dereferenceExpressionTree(streamsById)(expr).asInstanceOf[StreamExpression]
  }
}
