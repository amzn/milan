package com.amazon.milan.compiler.scala.event.operators

import com.amazon.milan.compiler.scala.event.{KeyedStateInterface, RecordWrapper}


abstract class ScanOperationHost[TIn, TFullKey, TKey, TState, TOut] {
  protected val scanOperation: ScanOperation[TIn, TKey, TState, TOut]

  protected val state: KeyedStateInterface[TFullKey, TState]

  protected def getLocalKey(fullKey: TFullKey): TKey

  def processRecord(record: RecordWrapper[TIn, TFullKey]): Option[RecordWrapper[TOut, TFullKey]] = {
    val currentState =
      this.state.getState(record.key) match {
        case Some(state) => state
        case None => this.scanOperation.initialState
      }

    val localKey = this.getLocalKey(record.key)
    val (newState, output) = this.scanOperation.process(currentState, record.value, localKey)
    this.state.setState(record.key, newState)

    Some(record.withValue(output))
  }
}
