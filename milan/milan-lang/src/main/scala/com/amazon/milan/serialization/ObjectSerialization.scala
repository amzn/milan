package com.amazon.milan.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, ObjectOutputStream}
import java.nio.file.{Files, Path}


object ObjectSerialization {
  /**
   * Deserializes an object from a byte array.
   *
   * @param bytes A byte array containing a serialized object.
   * @tparam T : The type of object to deserialize.
   * @return The deserialized object.
   */
  def deserialize[T](bytes: Array[Byte]): T = {
    deserialize(new ByteArrayInputStream(bytes))
  }

  /**
   * Deserializes an object from a stream.
   *
   * @param stream A stream containing a serialized object.
   * @tparam T : The type of object to deserialize.
   * @return The deserialized object.
   */
  def deserialize[T](stream: InputStream): T = {
    // Use our custom ObjectInputStream to get around some class loader issues that arise when running in Flink.
    val objectReader = new ApplicationObjectInputStream(stream)
    objectReader.readObject().asInstanceOf[T]
  }

  /**
   * Deserializes an object from a file.
   *
   * @param path : The path to the file containing the serialized object.
   * @tparam T : The type of object to deserialize.
   * @return The deserialized object.
   */
  def deserialize[T](path: Path): T = {
    deserialize(Files.readAllBytes(path))
  }

  /**
   * Serializes an object to a byte array.
   *
   * @param value The object to serialize.
   * @return A byte array containing the serialized object.
   * @tparam T : The type of the object being serialized.
   */
  def serialize[T](value: T): Array[Byte] = {
    val bytesStream = new ByteArrayOutputStream()
    val objectStream = new ObjectOutputStream(bytesStream)

    objectStream.writeObject(value)
    objectStream.close()

    bytesStream.close()
    bytesStream.toByteArray
  }

  /**
   * Serializes an object to a file.
   *
   * @param value The object to serialize.
   * @param path  The path where the serialized object will be written.
   * @tparam T The type of the object being serialized.
   */
  def serialize[T](value: T, path: Path): Unit = {
    val bytes = serialize(value)
    Files.write(path, bytes)
  }

  /**
   * Creates a copy of an object tree via serialization and deserialization.
   *
   * @param value The object to copy.
   * @tparam T The type of the object.
   * @return A copy of the object.
   */
  def copy[T](value: T): T = {
    deserialize(serialize(value))
  }
}
