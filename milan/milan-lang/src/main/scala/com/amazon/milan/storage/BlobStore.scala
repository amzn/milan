package com.amazon.milan.storage

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}


object BlobStore {

  /**
   * Helper methods for working with [[BlobStore]] objects.
   */
  implicit class BlobStoreExtensions(val blobStore: BlobStore) extends AnyVal {
    /**
     * Reads a full blob as a byte array.
     *
     * @param key The blob key.
     * @return A byte array of the blob contents.
     */
    def readBlobBytes(key: String): Array[Byte] = {
      val outputStream = new ByteArrayOutputStream()
      this.blobStore.readBlob(key, outputStream)
      outputStream.toByteArray
    }

    /**
     * Writes a blob from a byte array.
     *
     * @param key  The blob key.
     * @param blob A byte array of the blob contents.
     */
    def writeBlobBytes(key: String, blob: Array[Byte]): Unit = {
      val inputStream = new ByteArrayInputStream(blob)
      this.blobStore.writeBlob(key, inputStream)
    }
  }

}


/**
 * An interface for storing and reading blobs.
 */
trait BlobStore extends Serializable {
  /**
   * Read the blob with the specified key from the store.
   *
   * @param key A blob key.
   * @return The blob bytes.
   */
  def readBlob(key: String, targetStream: OutputStream): Unit

  /**
   * Write a blob with the specified key to the store.
   *
   * @param key        The blob key.
   * @param blobStream The blob stream.
   */
  def writeBlob(key: String, blobStream: InputStream): Unit
}


class BlobNotFoundException(key: String, cause: Throwable)
  extends Exception(s"Blob with key '$key' was not found.", cause) {

  def this(key: String) {
    this(key, null)
  }
}
