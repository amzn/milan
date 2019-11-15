package com.amazon.milan.storage

import java.io.{InputStream, OutputStream}

import scala.collection.mutable


/**
 * An implementation of [[BlobStore]] that keeps blobs in memory.
 *
 * @param name The name of the store.
 */
class MemoryBlobStore(val name: String) extends BlobStore {
  private val blobs = new mutable.HashMap[String, Array[Byte]]()

  /**
   * Initializes a new instance of the [[MemoryBlobStore]] class with no name.
   */
  def this() {
    this("")
  }

  override def readBlob(key: String, targetStream: OutputStream): Unit = {
    if (!this.blobs.contains(key)) {
      throw new BlobNotFoundException(key)
    }

    targetStream.write(this.blobs(key))
  }

  override def writeBlob(key: String, blobStream: InputStream): Unit = {
    val bytes = IO.readAllBytes(blobStream)
    this.blobs.put(key, bytes)
  }

  /**
   * Get the blob with the specified key.
   *
   * @param key A blob key.
   * @return A byte array containing the blob contents.
   */
  def getBlob(key: String): Array[Byte] = this.blobs(key)
}
