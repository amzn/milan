package com.amazon.milan.storage

import scala.collection.mutable


/**
 * An implementation of [[BlobStoreFactory]] that creates [[MemoryBlobStore]] objects.
 */
class MemoryBlobStoreFactory extends BlobStoreFactory {
  private val stores = new mutable.HashMap[String, MemoryBlobStore]()

  override def createBlobStore(name: String): BlobStore = {
    this.stores.getOrElseUpdate(name, new MemoryBlobStore(name))
  }

  def getStore(name: String): MemoryBlobStore = this.stores(name)
}
