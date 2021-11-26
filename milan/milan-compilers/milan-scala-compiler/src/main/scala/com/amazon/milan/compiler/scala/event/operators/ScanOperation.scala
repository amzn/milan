package com.amazon.milan.compiler.scala.event.operators

import com.amazon.milan.typeutil.TypeDescriptor


trait ScanOperation[TIn, TKey, TState, TOut] extends Serializable {
  val initialState: TState

  val stateTypeDescriptor: TypeDescriptor[TState]

  val outputTypeDescriptor: TypeDescriptor[TOut]

  def process(state: TState, input: TIn, key: TKey): (TState, TOut)
}


object ScanOperation {
  def compose[TIn, TKey, TFirstState, TFirstOut, TSecondState, TSecondOut](first: ScanOperation[TIn, TKey, TFirstState, TFirstOut],
                                                                           second: ScanOperation[TFirstOut, TKey, TSecondState, TSecondOut]) =
    new ComposedScanOperations[TIn, TKey, TFirstState, TFirstOut, TSecondState, TSecondOut](first, second)
}


class ComposedScanOperations[TIn, TKey, TFirstState, TFirstOut, TSecondState, TSecondOut](first: ScanOperation[TIn, TKey, TFirstState, TFirstOut],
                                                                                          second: ScanOperation[TFirstOut, TKey, TSecondState, TSecondOut])
  extends ScanOperation[TIn, TKey, (TFirstState, TSecondState), TSecondOut] {

  override val initialState: (TFirstState, TSecondState) = (this.first.initialState, this.second.initialState)

  override val stateTypeDescriptor: TypeDescriptor[(TFirstState, TSecondState)] =
    TypeDescriptor.createTuple[(TFirstState, TSecondState)](List(this.first.stateTypeDescriptor, this.second.stateTypeDescriptor))

  override val outputTypeDescriptor: TypeDescriptor[TSecondOut] =
    this.second.outputTypeDescriptor

  override def process(state: (TFirstState, TSecondState), input: TIn, key: TKey): ((TFirstState, TSecondState), TSecondOut) = {
    val (firstState, secondState) = state
    val (newFirstState, firstOutput) = this.first.process(firstState, input, key)
    val (newSecondState, secondOutput) = this.second.process(secondState, firstOutput, key)
    ((newFirstState, newSecondState), secondOutput)
  }
}
