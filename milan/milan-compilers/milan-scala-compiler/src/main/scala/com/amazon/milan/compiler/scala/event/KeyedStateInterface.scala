package com.amazon.milan.compiler.scala.event

/**
 * An interface to keyed state storage.
 *
 * @tparam TKey   The key type.
 * @tparam TState The state type.
 */
trait KeyedStateInterface[TKey, TState] {
  /**
   * Gets the state value for a key, or None if no value is present.
   *
   * @param key A key
   * @return The state value, or None if no value is present for the key.
   */
  def getState(key: TKey): Option[TState]

  /**
   * Sets the state value for a key.
   *
   * @param key   A key.
   * @param state The state for the specified key.
   */
  def setState(key: TKey, state: TState): Unit
}
