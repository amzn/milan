package com.amazon.milan.storage

import scala.collection.mutable
import scala.reflect.ClassTag


/**
 * An implementation of [[EntityStoreFactory]] that creates [[MemoryEntityStore]] objects.
 */
class MemoryEntityStoreFactory extends EntityStoreFactory {
  val stores = new mutable.HashMap[String, MemoryEntityStore[_]]()

  override def createEntityStore[T: ClassTag](name: String): EntityStore[T] = {
    stores.getOrElseUpdate(name, new MemoryEntityStore[T](name)).asInstanceOf[MemoryEntityStore[T]]
  }

  def getStore[T](name: String): MemoryEntityStore[T] = this.stores(name).asInstanceOf[MemoryEntityStore[T]]
}
