package com.amazon.milan.program

import com.amazon.milan.typeutil.StreamTypeDescriptor
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * Trait for streams.
 */
// The base GraphNode class defines a custom serializer and deserializer, which relies on calling the default
// deserializer for derived classes. Java annotations are inherited so we need an empty JsonDeserialize annotation to
// tell Jackson to use the default deserializer.
trait Stream extends GraphNode {
  @JsonIgnore
  def getStreamExpression: StreamExpression

  override def getExpression: GraphNodeExpression =
    this.getStreamExpression

  override def isStream: Boolean = true

  def withName(name: String): Stream =
    this.withNameAndId(name, this.nodeId)

  def withId(id: String): Stream = {
    if (this.nodeId == this.name) {
      this.withNameAndId(id, id)
    }
    else {
      this.withNameAndId(this.name, id)
    }
  }

  protected def withNameAndId(name: String, id: String): Stream
}


/**
 * A stream with a source that is external to the program.
 */
@JsonSerialize
@JsonDeserialize
case class ExternalStream(nodeId: String, name: String, streamType: StreamTypeDescriptor)
  extends Stream {

  override def getStreamExpression: StreamExpression = {
    val expr = new Ref(this.nodeId, this.name)
    expr.tpe = this.streamType
    expr
  }

  override def withNameAndId(name: String, id: String): Stream =
    ExternalStream(id, name, this.streamType)
}


/**
 * A graph node that is computed from other streams.
 */
@JsonSerialize
@JsonDeserialize
case class ComputedGraphNode(nodeId: String, definition: GraphNodeExpression)
  extends GraphNode {

  val name: String = nodeId

  override def getExpression: GraphNodeExpression = this.definition
}


/**
 * A stream that is computed from other streams.
 */
@JsonSerialize
@JsonDeserialize
case class ComputedStream(nodeId: String, name: String, definition: StreamExpression)
  extends Stream {

  override def getStreamExpression: StreamExpression = this.definition

  override def withNameAndId(name: String, id: String): Stream =
    ComputedStream(id, name, this.definition.withNameAndId(name, id).asInstanceOf[StreamExpression])
}
