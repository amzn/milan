package com.amazon.milan.storage


/**
 * Interface to a store of entities of a single type.
 *
 * @tparam T The type of entity.
 */
trait EntityStore[T] extends Serializable {
  def getEntity(key: String): T

  def putEntity(key: String, value: T): Unit

  def entityExists(key: String): Boolean

  def listEntityKeys(): TraversableOnce[String]

  def listEntities(): TraversableOnce[T]
}


class EntityNotFoundException(val key: String, cause: Throwable)
  extends Exception(s"Entity with key '$key' was not found.", cause) {

  def this(key: String) {
    this(key, null)
  }
}
