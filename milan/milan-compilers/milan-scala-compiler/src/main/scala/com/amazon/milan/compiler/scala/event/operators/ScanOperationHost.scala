package com.amazon.milan.compiler.scala.event.operators

import com.amazon.milan.compiler.scala.event.{KeyedStateInterface, RecordWrapper}


abstract class ScanOperationHost[TIn, TKey, TState, TOut] {
  protected val scanOperation: ScanOperation[TIn, TState, TOut]

  protected val state: KeyedStateInterface[TKey, TState]

  def processRecord(record: RecordWrapper[TIn, TKey]): Option[RecordWrapper[TOut, TKey]] = {
    val currentState =
      this.state.getState(record.key) match {
        case Some(state) => state
        case None => this.scanOperation.initialState
      }

    val (newState, output) = this.scanOperation.process(currentState, record.value)
    this.state.setState(record.key, newState)

    output.map(value => record.withValue(value))
  }
}
