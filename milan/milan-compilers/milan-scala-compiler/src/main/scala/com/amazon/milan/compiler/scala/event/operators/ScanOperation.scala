package com.amazon.milan.compiler.scala.event.operators

import com.amazon.milan.typeutil.TypeDescriptor


trait ScanOperation[TIn, TState, TOut] extends Serializable {
  val initialState: TState

  val stateTypeDescriptor: TypeDescriptor[TState]

  val outputTypeDescriptor: TypeDescriptor[TOut]

  def process(state: TState, input: TIn): (TState, Option[TOut])
}


object ScanOperation {
  def compose[TIn, TFirstState, TFirstOut, TSecondState, TSecondOut](first: ScanOperation[TIn, TFirstState, TFirstOut],
                                                                     second: ScanOperation[TFirstOut, TSecondState, TSecondOut]) =
    new ComposedScanOperations[TIn, TFirstState, TFirstOut, TSecondState, TSecondOut](first, second)
}


class ComposedScanOperations[TIn, TFirstState, TFirstOut, TSecondState, TSecondOut](first: ScanOperation[TIn, TFirstState, TFirstOut],
                                                                                    second: ScanOperation[TFirstOut, TSecondState, TSecondOut])
  extends ScanOperation[TIn, (TFirstState, TSecondState), TSecondOut] {

  override val initialState: (TFirstState, TSecondState) = (this.first.initialState, this.second.initialState)

  override val stateTypeDescriptor: TypeDescriptor[(TFirstState, TSecondState)] =
    TypeDescriptor.createTuple[(TFirstState, TSecondState)](List(this.first.stateTypeDescriptor, this.second.stateTypeDescriptor))

  override val outputTypeDescriptor: TypeDescriptor[TSecondOut] =
    this.second.outputTypeDescriptor

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
}
