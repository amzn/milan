package com.amazon.milan.flink.components

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}


object ObjectStreamUtil {
  def serializeAndDeserialize[T](value: T): T = {
    val outputStream = new ByteArrayOutputStream()
    val objectOutputStream = new ObjectOutputStream(outputStream)
    objectOutputStream.writeObject(value)

    val bytes = outputStream.toByteArray
    val objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes))
    objectInputStream.readObject().asInstanceOf[T]
  }
}
