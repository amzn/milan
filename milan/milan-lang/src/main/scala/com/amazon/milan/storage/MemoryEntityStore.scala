package com.amazon.milan.storage

import scala.collection.mutable


/**
 * An implementation of [[EntityStore]] that stores entities in memory.
 *
 * @param name The name of the store.
 * @tparam T The type of entity.
 */
class MemoryEntityStore[T](val name: String) extends EntityStore[T] {
  private val entities = new mutable.HashMap[String, T]()

  /**
   * Initializes a new instance of the [[MemoryEntityStore]] class with no name.
   */
  def this() {
    this("")
  }

  override def entityExists(key: String): Boolean = {
    this.entities.contains(key)
  }

  override def getEntity(key: String): T = {
    if (!this.entityExists(key)) {
      throw new EntityNotFoundException(key)
    }

    this.entities(key)
  }

  override def listEntities(): TraversableOnce[T] = {
    this.entities.values
  }

  override def listEntityKeys(): TraversableOnce[String] = {
    this.entities.keys
  }

  override def putEntity(key: String, value: T): Unit = {
    this.entities.put(key, value)
  }
}
