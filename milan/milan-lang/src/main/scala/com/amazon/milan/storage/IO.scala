package com.amazon.milan.storage

import java.io.{ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

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

  /**
   * Reads the entire content of a stream as a string.
   *
   * @param stream A stream.
   * @return The full content of the stream, decoded as a string.
   */
  def readAllString(stream: InputStream): String = {
    StandardCharsets.UTF_8.decode(ByteBuffer.wrap(this.readAllBytes(stream))).toString
  }
}
