package com.amazon.milan.storage

import java.nio.file.Path

import com.amazon.milan.serialization.ScalaObjectMapper

import scala.reflect.ClassTag


/**
 * [[EntityStoreFactory]] implementation that stores entities on the local file system.
 *
 * @param rootPath The root path of entity stores created by this factory.
 */
class LocalFolderEntityStoreFactory(rootPath: Path) extends EntityStoreFactory {
  /**
   * Create an [[EntityStore]] for the specified entity type.
   *
   * @param name The name of the store.
   * @tparam T The type of entity.
   * @return An [[EntityStore]]`[`T`]` interface for the requested entity store.
   */
  override def createEntityStore[T: ClassTag](name: String): EntityStore[T] = {
    val storePath = this.rootPath.resolve(name + "/")
    new LocalFolderEntityStore[T](storePath, ScalaObjectMapper)
  }
}
