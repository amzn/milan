package com.amazon.milan.compiler.flink.testing

import java.io.InputStream

class NonClosingInputStream(stream: InputStream) extends InputStream {
  override def read(): Int = stream.read()

  override def read(b: Array[Byte]): Int = stream.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = stream.read(b, off, len)

  override def close(): Unit = ()

  override def skip(n: Long): Long = stream.skip(n)

  override def available(): Int = stream.available()

  override def mark(readlimit: Int): Unit = stream.mark(readlimit)

  override def markSupported(): Boolean = stream.markSupported()

  override def reset(): Unit = stream.reset()
}
