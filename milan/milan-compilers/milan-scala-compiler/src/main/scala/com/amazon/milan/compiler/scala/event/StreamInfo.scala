package com.amazon.milan.compiler.scala.event

import com.amazon.milan.program.StreamExpression
import com.amazon.milan.typeutil.{StreamTypeDescriptor, TupleTypeDescriptor, TypeDescriptor, types}


case class StreamInfo(expr: StreamExpression,
                      contextKeyType: TupleTypeDescriptor[_],
                      keyType: TypeDescriptor[_]) {

  /**
   * Gets the full key type of the stream, combining the context key type and any key type introduced by the stream.
   */
  def fullKeyType: TupleTypeDescriptor[_] = TypeDescriptor.augmentTuple(this.contextKeyType, this.keyType)

  /**
   * Gets the record type of the stream.
   */
  def recordType: TypeDescriptor[_] = expr.recordType

  /**
   * Gets the type of the stream.
   *
   * @return
   */
  def streamType: StreamTypeDescriptor = expr.tpe.asStream

  /**
   * Gets the ID of the stream.
   */
  def streamId: String = expr.nodeId

  /**
   * Gets a new [[StreamInfo]] that is equivalent to this one, but with the expression replaced with the specified
   * expression.
   */
  def withExpression(newExpr: StreamExpression): StreamInfo = {
    StreamInfo(newExpr, this.contextKeyType, this.keyType)
  }

  /**
   * Gets whether the stream is keyed.
   */
  def isKeyed: Boolean = (keyType != types.EmptyTuple) || contextKeyType.fields.nonEmpty

  /**
   * Gets a new [[StreamInfo]] equivalent to this one with the specified key type.
   */
  def withKeyType(newKeyType: TypeDescriptor[_]): StreamInfo = {
    StreamInfo(this.expr, this.contextKeyType, newKeyType)
  }

  /**
   * Gets a new [[StreamInfo]] equivalent to this one with the specified context type.
   */
  def withContextKeyType(newContextKeyType: TupleTypeDescriptor[_]): StreamInfo = {
    StreamInfo(this.expr, newContextKeyType, this.keyType)
  }

  /**
   * Gets a new [[StreamInfo]] that is equivalent to this [[StreamInfo]] with an additional element added to the
   * context key.
   */
  def addContextKeyType(newKeyElement: TypeDescriptor[_]): StreamInfo = {
    val newContextKeyType = TypeDescriptor.augmentTuple(this.contextKeyType, newKeyElement)
    StreamInfo(this.expr, newContextKeyType, this.keyType)
  }

  /**
   * Adds the current key to the context key in a new [[StreamInfo]].
   */
  def addKeyToContext(): StreamInfo = {
    if (this.keyType == types.EmptyTuple) {
      this
    }
    else {
      this.addContextKeyType(this.keyType).withKeyType(types.EmptyTuple)
    }
  }
}
