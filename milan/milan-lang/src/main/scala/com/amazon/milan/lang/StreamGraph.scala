package com.amazon.milan.lang

import com.amazon.milan.program
import com.amazon.milan.program.{ComputedStream, ExternalStream, Filter, GraphNode, GraphNodeExpression, MapNodeExpression, Ref, StreamExpression, Tree}
import com.fasterxml.jackson.annotation.{JsonCreator, JsonIgnore}

import scala.language.experimental.macros


@JsonCreator()
class StreamGraph(var streamsById: Map[String, program.Stream]) {
  def this() {
    this(Map.empty[String, program.Stream])
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
    this.addWithReferences(stream.node)
  }

  /**
   * Gets the stream with the specified ID.
   *
   * @param streamId The ID of a stream.
   * @return The [[program.Stream]] object representing the requested stream.
   */
  def getStream(streamId: String): program.Stream = {
    this.streamsById.get(streamId) match {
      case Some(stream) =>
        stream.asStream

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
  def getStreams: Iterable[program.Stream] = this.streamsById.values

  /**
   * Gets the nodes in the graph with all references replaced with actual nodes where possible.
   */
  @JsonIgnore
  def getDereferencedGraph: StreamGraph = {
    // We need a map of IDs to stream expressions, where the expressions have had node IDs applied.
    val streamExpressionsById = this.streamsById.map {
      case (id, stream) => id -> this.getExpressionWithNameAndId(stream)
    }

    val dereferencedStreams =
      streamExpressionsById.values
        .map(this.dereferenceGraphNodeExpression(streamExpressionsById))
        .map(this.convertToStream)
        .map(stream => stream.nodeId -> stream)
        .toMap

    new StreamGraph(dereferencedStreams)
  }

  /**
   * Recursively adds a [[program.Stream]] object to the graph, replacing input streams with references.
   *
   * @param stream A [[program.Stream]] object to add to the graph.
   */
  private def addWithReferences(stream: program.Stream): Unit = {
    if (this.streamsById.contains(stream.nodeId)) {
      return
    }

    // First add any child graph nodes as separate entries in the database.
    stream.getExpression.getChildren
      .flatMap(Tree.getStreamExpressions)
      .map(this.convertToStream)
      .foreach(this.addWithReferences)

    // Get a version of the node with child nodes replaced with references.
    // This prevents us from deep-copying any nodes.
    val exprWithReferences = Tree.replaceChildStreamsWithReferences(stream.getExpression).asInstanceOf[StreamExpression]
    val streamToAdd = this.convertToStream(exprWithReferences)

    this.streamsById = this.streamsById + (stream.nodeId -> streamToAdd)
  }

  /**
   * Reconstructs an expression tree so that any [[Ref]] nodes are replaced with the actual nodes
   * they refer to. Any references to external streams will remain as [[Ref]] nodes.
   *
   * @param tree An expression tree.
   * @return A copy of the tree with all non-external references replaced with actual nodes.
   */
  private def dereferenceExpressionTree(refsById: Map[String, GraphNodeExpression])(tree: Tree): Tree = {
    tree match {
      case Ref(nodeId) =>
        refsById(nodeId) match {
          case Ref(derefNodeId) =>
            refsById(derefNodeId)

          case expr =>
            this.dereferenceExpressionTree(refsById)(expr)
        }

      case _ =>
        val newChildren = tree.getChildren.map(this.dereferenceExpressionTree(refsById)).toList
        tree.replaceChildren(newChildren)
    }
  }

  /**
   * Reconstructs an expression tree so that any [[Ref]] nodes are replaced with the actual nodes
   * they refer to. Any references to external streams will remain as [[Ref]] nodes.
   *
   * @param expr A [[GraphNodeExpression]] expression tree.
   * @return A copy of the tree with all non-external references replaced with actual nodes.
   */
  private def dereferenceGraphNodeExpression(refsById: Map[String, GraphNodeExpression])(expr: GraphNodeExpression): GraphNodeExpression = {
    this.dereferenceExpressionTree(refsById)(expr).asInstanceOf[GraphNodeExpression]
  }

  /**
   * Gets a [[GraphNode]] representing the application of a [[GraphNodeExpression]] expression.
   */
  private def convertToStream(expr: GraphNodeExpression): program.Stream = {
    expr match {
      case r: Ref => ExternalStream(r.nodeId, r.nodeName, r.tpe.asStream)
      case f: Filter => ComputedStream(f.nodeId, f.nodeName, f)
      case m: MapNodeExpression => ComputedStream(m.nodeId, m.nodeName, m)
    }
  }

  /**
   * Gets the expression from a graph node with the name and ID of the node applied to the expression.
   */
  private def getExpressionWithNameAndId(stream: program.Stream): GraphNodeExpression = {
    stream match {
      case e: ExternalStream =>
        e.getExpression

      case s: ComputedStream =>
        s.definition.withNameAndId(s.name, s.nodeId)
    }
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
