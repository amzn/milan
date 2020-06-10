package com.amazon.milan.flink.runtime

import com.amazon.milan.flink.types.OptionTypeInformation
import org.apache.flink.api.common.typeinfo.TypeInformation


abstract class ScanOperationBase[TIn, TState, TOut](stateTypeInformation: TypeInformation[TState],
                                                    outputTypeInformation: TypeInformation[TOut])
  extends ScanOperation[TIn, TState, TOut] {

  override def getStateTypeInformation: TypeInformation[TState] = this.stateTypeInformation

  override def getOutputTypeInformation: TypeInformation[TOut] = this.outputTypeInformation
}


abstract class AssociativeScanOperation[T, TArg: Numeric, TOut](argTypeInformation: TypeInformation[TArg],
                                                                outputTypeInformation: TypeInformation[TOut])
  extends ScanOperationBase[T, TArg, TOut](argTypeInformation, outputTypeInformation) {

  @transient private lazy val argNumeric = implicitly[Numeric[TArg]]

  protected def getInitialState(numeric: Numeric[TArg]): TArg

  protected def getArg(input: T): TArg

  protected def getOutput(input: T, arg: TArg): TOut

  protected def add(numeric: Numeric[TArg], arg1: TArg, arg2: TArg): TArg

  override def getInitialState: TArg =
    this.getInitialState(this.argNumeric)

  override def process(state: TArg, input: T): (TArg, Option[TOut]) = {
    val inputArg = this.getArg(input)

    val newState = this.add(this.argNumeric, state, inputArg)
    val output = this.getOutput(input, newState)

    (newState, Some(output))
  }

  override def mergeStates(state1: TArg, state2: TArg): TArg = {
    this.add(this.argNumeric, state1, state2)
  }
}


abstract class ArgMaxScanOperation[T, TArg: Ordering](recordTypeInformation: TypeInformation[T],
                                                      argTypeInformation: TypeInformation[TArg])
  extends ScanOperationBase[T, Option[TArg], T](
    OptionTypeInformation.wrap(argTypeInformation),
    recordTypeInformation) {

  @transient private lazy val argOrdering = implicitly[Ordering[TArg]]

  protected def greaterThan(ordering: Ordering[TArg], arg1: TArg, arg2: TArg): Boolean

  protected def getArg(input: T): TArg

  override def getInitialState: Option[TArg] = None

  override def mergeStates(state1: Option[TArg], state2: Option[TArg]): Option[TArg] = {
    (state1, state2) match {
      case (None, None) =>
        None

      case (Some(_), None) =>
        state1

      case (None, Some(_)) =>
        state2

      case (Some(arg1), Some(arg2)) =>
        if (greaterThan(this.argOrdering, arg1, arg2)) {
          state1
        }
        else {
          state2
        }
    }
  }

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
}
