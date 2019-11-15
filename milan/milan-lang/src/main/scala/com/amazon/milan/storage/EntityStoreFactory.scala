package com.amazon.milan.storage

import scala.reflect.ClassTag


/**
 * Factory interface for creating [[EntityStore]] objects for specific types.
 */
trait EntityStoreFactory extends Serializable {
  /**
   * Create an [[EntityStore]] for the specified entity type.
   *
   * @param name The name of the store.
   * @tparam T The type of entity.
   * @return An [[EntityStore]]`[`T`]` interface for the requested entity store.
   */
  def createEntityStore[T: ClassTag](name: String): EntityStore[T]
}
