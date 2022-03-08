package com.amazon.milan.aws.serverless

import com.amazon.milan.compiler.scala.event.KeyedStateInterface
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


/**
 * A [[KeyedStateInterface]] that stores state in a DynamoDb table.
 *
 * @param objectStore An [[ObjectStore]] interface to use for storing state.
 * @tparam TKey   The key type.
 * @tparam TState The state type.
 */
class ObjectStoreKeyedStateInterface[TKey, TState](objectStore: ObjectStore[TKey, TState])
  extends KeyedStateInterface[TKey, TState] {

  private lazy val logger = Logger(LoggerFactory.getLogger(this.getClass))

  override def getState(key: TKey): Option[TState] = {
    this.logger.info(s"Getting state for key '$key'.")
    this.objectStore.getItem(key)
  }

  override def setState(key: TKey, state: TState): Unit = {
    this.logger.info(s"Setting state for key '$key'.")
    this.objectStore.putItem(key, state)
  }
}
