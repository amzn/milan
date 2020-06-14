package com.amazon.milan.compiler.flink.runtime

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.FunctionInitializationContext


class SequenceNumberHelper(runtimeContext: RuntimeContext) {
  private val nextSequenceNumber: ValueState[Long] = {
    val nextSequenceNumberDescriptor = new ValueStateDescriptor[Long]("nextSequenceNumber", createTypeInformation[Long])
    runtimeContext.getState(nextSequenceNumberDescriptor)
  }

  def increment(): Long = {
    val sequenceNumber = this.nextSequenceNumber.value()
    this.nextSequenceNumber.update(sequenceNumber + 1)
    sequenceNumber
  }
}


object OperatorSequenceNumberHelper {
  def create(context: FunctionInitializationContext): OperatorSequenceNumberHelper = {
    val stateDescriptor = new ListStateDescriptor[Long]("nextSequenceNumber", createTypeInformation[Long])
    val state = context.getOperatorStateStore.getListState(stateDescriptor)

    val nextSequenceNumber =
      if (context.isRestored) {
        state.get().iterator().next()
      }
      else {
        0
      }

    new OperatorSequenceNumberHelper(nextSequenceNumber, state)
  }
}

class OperatorSequenceNumberHelper(private var nextSequenceNumber: Long, checkpointState: ListState[Long]) extends Serializable {
  def increment(): Long = {
    val sequenceNumber = this.nextSequenceNumber
    this.nextSequenceNumber += 1
    sequenceNumber
  }

  def snapshotState(): Unit = {
    this.checkpointState.clear()
    this.checkpointState.add(this.nextSequenceNumber)
  }
}
