package com.amazon.milan.storage

import java.io.{ByteArrayOutputStream, InputStream}

import org.apache.commons.io.IOUtils


object IO {
  /**
   * Read all bytes from a stream.
   *
   * @param stream A stream.
   * @return An array of bytes read from the stream.
   */
  def readAllBytes(stream: InputStream): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    IOUtils.copy(stream, outputStream)
    outputStream.toByteArray
  }
}
