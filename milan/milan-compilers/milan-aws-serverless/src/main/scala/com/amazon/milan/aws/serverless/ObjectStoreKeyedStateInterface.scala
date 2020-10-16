package com.amazon.milan.aws.serverless

import com.amazon.milan.compiler.scala.event.KeyedStateInterface


/**
 * A [[KeyedStateInterface]] that stores state in a DynamoDb table.
 *
 * @param objectStore An [[ObjectStore]] interface to use for storing state.
 * @tparam TKey   The key type.
 * @tparam TState The state type.
 */
class ObjectStoreKeyedStateInterface[TKey, TState](objectStore: ObjectStore[TKey, TState],
                                                   defaultValue: TState)
  extends KeyedStateInterface[TKey, TState] {

  override def getState(key: TKey): Option[TState] = {
    this.objectStore.getItem(key) match {
      case Some(item) => Some(item)
      case None => Some(defaultValue)
    }
  }

  override def setState(key: TKey, state: TState): Unit = {
    this.objectStore.putItem(key, state)
  }
}
