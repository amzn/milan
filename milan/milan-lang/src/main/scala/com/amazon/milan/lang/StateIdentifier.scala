package com.amazon.milan.lang


object StateIdentifier {
  val JOIN_LEFT_STATE = "left"
  val JOIN_RIGHT_STATE = "right"
  val STREAM_STATE = "state"
}


/**
 * Identifies state storage required for an operation.
 *
 * Stateful operations will provide one more state identifiers which identify the different state storage
 * required for the operation.
 */
class StateIdentifier(val stateId: String) {
  override def equals(obj: Any): Boolean = obj match {
    case p: StateIdentifier => this.stateId == p.stateId
    case _ => false
  }

  override def hashCode(): Int = this.stateId.hashCode

  override def toString: String = this.stateId.toString
}
