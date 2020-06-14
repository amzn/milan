package com.amazon.milan.compiler.flink.runtime

import org.apache.flink.api.common.typeinfo.TypeInformation


/**
 * A Flink KeyedProcessFunction that executes a [[ScanOperation]].
 */
abstract class ScanOperationKeyedProcessFunction[T >: Null, TKey >: Null <: Product, TState, TOut >: Null](scanOperation: ScanOperation[T, TState, TOut],
                                                                                                           keyTypeInfo: TypeInformation[TKey])
  extends ScanKeyedProcessFunction[T, TKey, TState, TOut](scanOperation.getInitialState, keyTypeInfo, scanOperation.getStateTypeInformation, scanOperation.getOutputTypeInformation) {

  override protected def process(state: TState, key: TKey, value: T): (TState, Option[TOut]) = {
    this.scanOperation.process(state, value)
  }
}


/**
 * A Flink ProcessFunction that executes a [[ScanOperation]].
 */
abstract class ScanOperationProcessFunction[T >: Null, TKey >: Null <: Product, TState, TOut >: Null](scanOperation: ScanOperation[T, TState, TOut],
                                                                                                      keyTypeInfo: TypeInformation[TKey])
  extends ScanProcessFunction[T, TKey, TState, TOut](scanOperation.getInitialState, keyTypeInfo, scanOperation.getStateTypeInformation, scanOperation.getOutputTypeInformation) {

  override protected def process(state: TState, value: T): (TState, Option[TOut]) = {
    this.scanOperation.process(state, value)
  }
}
