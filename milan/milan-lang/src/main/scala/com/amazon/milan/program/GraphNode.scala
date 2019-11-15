package com.amazon.milan.program

import com.amazon.milan.serialization.{TypeInfoProvider, TypedJsonDeserializer, TypedJsonSerializer}
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * Trait for all nodes that are part of the data flow graph.
 */
@JsonSerialize(using = classOf[GraphNodeSerializer])
@JsonDeserialize(using = classOf[GraphNodeDeserializer])
trait GraphNode extends TypeInfoProvider {
  val _type: String = getClass.getSimpleName
  val nodeId: String
  val name: String

  /**
   * Gets the [[GraphNodeExpression]] that defines this node.
   * For most nodes this will be the Milan expression that defines the node's relationships to other nodes.
   * For external streams this will be a [[Ref]] referencing the stream by name.
   */
  @JsonIgnore
  def getExpression: GraphNodeExpression

  @JsonIgnore
  override def getJsonTypeName: String = this._type

  /**
   * Gets a [[Ref]] object that refers to this node.
   */
  @JsonIgnore
  def getReference: Ref = new Ref(this.nodeId, this.name)

  /**
   * Gets a version of this [[GraphNode]] with its inputs replaced with [[Ref]] objects that refer to the
   * input nodes by ID.
   *
   * @return A version of this [[GraphNode]] with the inputs replaced with references.
   */
  def replaceInputsWithReferences(): GraphNode = this

  /**
   * Gets a version of this [[GraphNode]] with its [[Ref]] inputs replaced with their referenced objects.
   * This is a recursive operation that applies the same transformation to the input nodes of this node.
   *
   * @param nodes A map of node ID to [[GraphNode]] objects.
   * @return A version of this [[GraphNode]] with the inputs replaced with their referenced objects.
   */
  def replaceReferencesWithNodes(nodes: Map[String, GraphNode]): GraphNode = this

  /**
   * Gets whether this graph node represents a stream.
   *
   * @return True if the node represents a stream, otherwise false.
   */
  def isStream: Boolean = false

  /**
   * Gets this graph node as a [[Stream]].
   * If the node does not represent a stream, an exception is thrown.
   *
   * @return The node reference converted to a [[Stream]].
   */
  def asStream: Stream = {
    if (!isStream) {
      throw new UnsupportedOperationException()
    }

    this.asInstanceOf[Stream]
  }
}

class GraphNodeSerializer extends TypedJsonSerializer[GraphNode]

class GraphNodeDeserializer extends TypedJsonDeserializer[GraphNode]("com.amazon.milan.program")
