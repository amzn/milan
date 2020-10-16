package com.amazon.milan.compiler.scala.event

import scala.collection.mutable


/**
 * An implementation of [[KeyedStateInterface]] that stores state in memory.
 *
 * @tparam TKey   The key type.
 * @tparam TState The state type.
 */
class MemoryKeyedState[TKey, TState] extends KeyedStateInterface[TKey, TState] {
  private val state = mutable.HashMap.empty[TKey, TState]

  override def getState(key: TKey): Option[TState] = {
    this.state.get(key)
  }

  override def setState(key: TKey, state: TState): Unit = {
    this.state.put(key, state)
  }
}
