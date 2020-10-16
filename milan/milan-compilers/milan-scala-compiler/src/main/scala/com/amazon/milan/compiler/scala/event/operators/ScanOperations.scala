package com.amazon.milan.compiler.scala.event.operators

import com.amazon.milan.typeutil.TypeDescriptor


abstract class ScanOperationBase[TIn, TState, TOut](override val stateTypeDescriptor: TypeDescriptor[TState],
                                                    override val outputTypeDescriptor: TypeDescriptor[TOut])
  extends ScanOperation[TIn, TState, TOut] {
}


abstract class AssociativeScanOperation[T, TArg: Numeric, TOut](argTypeDescriptor: TypeDescriptor[TArg],
                                                                outputTypeDescriptor: TypeDescriptor[TOut])
  extends ScanOperationBase[T, TArg, TOut](argTypeDescriptor, outputTypeDescriptor) {

  protected val argNumeric: Numeric[TArg] = implicitly[Numeric[TArg]]

  override def process(state: TArg, input: T): (TArg, Option[TOut]) = {
    val inputArg = this.getArg(input)

    val newState = this.add(this.argNumeric, state, inputArg)
    val output = this.getOutput(input, newState)

    (newState, Some(output))
  }

  protected def getArg(input: T): TArg

  protected def getOutput(input: T, arg: TArg): TOut

  protected def add(numeric: Numeric[TArg], arg1: TArg, arg2: TArg): TArg
}


abstract class ArgMaxScanOperation[T, TArg: Ordering](recordTypeDescriptor: TypeDescriptor[T],
                                                      argTypeDescriptor: TypeDescriptor[TArg])
  extends ScanOperationBase[T, Option[TArg], T](
    TypeDescriptor.optionOf(argTypeDescriptor),
    recordTypeDescriptor) {

  @transient private lazy val argOrdering = implicitly[Ordering[TArg]]

  override val initialState: Option[TArg] = None

  override def process(state: Option[TArg], input: T): (Option[TArg], Option[T]) = {
    val inputArg = this.getArg(input)

    state match {
      case None =>
        // No previous state means this is the first value we've seen, so output it.
        (Some(inputArg), Some(input))

      case Some(stateArg) =>
        if (this.greaterThan(this.argOrdering, inputArg, stateArg)) {
          // This value has a greater arg than the previous greatest value, so output it.
          (Some(inputArg), Some(input))
        }
        else {
          // This value has a smaller arg than the previous greatest value, so don't output it.
          (state, None)
        }
    }
  }

  protected def greaterThan(ordering: Ordering[TArg], arg1: TArg, arg2: TArg): Boolean

  protected def getArg(input: T): TArg
}
