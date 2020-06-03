package com.amazon.milan.flink.runtime

import com.amazon.milan.flink.TypeUtil
import org.apache.flink.api.common.typeinfo.TypeInformation


trait ScanOperation[TIn, TState, TOut] extends Serializable {
  def getInitialState: TState

  def process(state: TState, input: TIn): (TState, Option[TOut])

  def mergeStates(state1: TState, state2: TState): TState

  def getStateTypeInformation: TypeInformation[TState]

  def getOutputTypeInformation: TypeInformation[TOut]
}


object ScanOperation {
  def compose[TIn, TFirstState, TFirstOut, TSecondState, TSecondOut](first: ScanOperation[TIn, TFirstState, TFirstOut],
                                                                     second: ScanOperation[TFirstOut, TSecondState, TSecondOut]) =
    new ComposedScanOperations[TIn, TFirstState, TFirstOut, TSecondState, TSecondOut](first, second)
}


class ComposedScanOperations[TIn, TFirstState, TFirstOut, TSecondState, TSecondOut](first: ScanOperation[TIn, TFirstState, TFirstOut],
                                                                                    second: ScanOperation[TFirstOut, TSecondState, TSecondOut])
  extends ScanOperation[TIn, (TFirstState, TSecondState), TSecondOut] {

  override def getInitialState: (TFirstState, TSecondState) = (this.first.getInitialState, this.second.getInitialState)

  override def process(state: (TFirstState, TSecondState), input: TIn): ((TFirstState, TSecondState), Option[TSecondOut]) = {
    val (firstState, secondState) = state
    val (newFirstState, firstOutput) = this.first.process(firstState, input)

    firstOutput match {
      case None =>
        // No output from the first function means we have nothing to send to the second function, so we output the new
        // first state and the previous second state.
        ((newFirstState, secondState), None)

      case Some(value) =>
        val (newSecondState, secondOutput) = this.second.process(secondState, value)

        // We don't care the the output was just send it along with the new states.
        ((newFirstState, newSecondState), secondOutput)
    }
  }

  override def mergeStates(state1: (TFirstState, TSecondState), state2: (TFirstState, TSecondState)): (TFirstState, TSecondState) = {
    val (firstState1, secondState1) = state1
    val (firstState2, secondState2) = state2
    val newState1 = this.first.mergeStates(firstState1, firstState2)
    val newState2 = this.second.mergeStates(secondState1, secondState2)
    (newState1, newState2)
  }

  override def getStateTypeInformation: TypeInformation[(TFirstState, TSecondState)] =
    TypeUtil.createTupleTypeInfo[(TFirstState, TSecondState)](this.first.getStateTypeInformation, this.second.getStateTypeInformation)

  override def getOutputTypeInformation: TypeInformation[TSecondOut] =
    second.getOutputTypeInformation
}
