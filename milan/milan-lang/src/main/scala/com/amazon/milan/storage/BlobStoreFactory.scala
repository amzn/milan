package com.amazon.milan.storage


/**
 * Interface for creating [[BlobStore]] objects.
 */
trait BlobStoreFactory extends Serializable {
  /**
   * Create a blob store with the specified name.
   *
   * @param name The name of the store.
   * @return A [[BlobStore]] interface to the store.
   */
  def createBlobStore(name: String): BlobStore
}
