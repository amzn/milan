package com.amazon.milan.compiler.scala.event.operators

import com.amazon.milan.typeutil.TypeDescriptor


abstract class ScanOperationBase[TIn, TKey, TState, TOut](override val stateTypeDescriptor: TypeDescriptor[TState],
                                                          override val outputTypeDescriptor: TypeDescriptor[TOut])
  extends ScanOperation[TIn, TKey, TState, TOut] {
}


/**
 * Base class for numeric scan operations that are associative, which means the order of operations does not matter.
 */
abstract class AssociativeScanOperation[T, TKey, TArg: Numeric, TOut](argTypeDescriptor: TypeDescriptor[TArg],
                                                                      outputTypeDescriptor: TypeDescriptor[TOut])
  extends ScanOperationBase[T, TKey, TArg, TOut](argTypeDescriptor, outputTypeDescriptor) {

  protected val argNumeric: Numeric[TArg] = implicitly[Numeric[TArg]]

  override def process(state: TArg, input: T, key: TKey): (TArg, TOut) = {
    val inputArg = this.getArg(input)

    val newState = this.add(this.argNumeric, state, inputArg)
    val output = this.getOutput(input, newState)

    (newState, output)
  }

  protected def getArg(input: T): TArg

  protected def getOutput(input: T, arg: TArg): TOut

  protected def add(numeric: Numeric[TArg], arg1: TArg, arg2: TArg): TArg
}


abstract class ArgMaxScanOperation[T, TKey, TArg: Ordering](recordTypeDescriptor: TypeDescriptor[T],
                                                            argTypeDescriptor: TypeDescriptor[TArg])
  extends ScanOperationBase[T, TKey, Option[(TArg, T)], T](
    TypeDescriptor.optionOf(argTypeDescriptor),
    recordTypeDescriptor) {

  @transient private lazy val argOrdering = implicitly[Ordering[TArg]]

  override val initialState: Option[(TArg, T)] = None

  override def process(state: Option[(TArg, T)], input: T, key: TKey): (Option[(TArg, T)], T) = {
    val inputArg = this.getArg(input)

    state match {
      case None =>
        // No previous state means this is the first value we've seen, so output it.
        (Some(inputArg, input), input)

      case Some((stateArg, stateVal)) =>
        if (this.greaterThan(this.argOrdering, inputArg, stateArg)) {
          // This value has a greater arg than the previous greatest value, so output it.
          (Some(inputArg, input), input)
        }
        else {
          // This value has a smaller arg than the previous greatest value, so output the previous value.
          (state, stateVal)
        }
    }
  }

  protected def greaterThan(ordering: Ordering[TArg], arg1: TArg, arg2: TArg): Boolean

  protected def getArg(input: T): TArg
}
