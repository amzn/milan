package com.amazon.milan.flink.runtime

import java.io.OutputStream

import com.amazon.milan.dataformats.DataOutputFormat
import org.apache.flink.api.common.serialization.Encoder


/**
 * A Flink [[Encoder]] that uses a Milan [[DataOutputFormat]] to encode values.
 *
 * @param format A [[DataOutputFormat]] that performs the encoding.
 */
class DataOutputFormatEncoder[T](format: DataOutputFormat[T]) extends Encoder[T] {
  override def encode(in: T, outputStream: OutputStream): Unit = {
    this.format.writeValue(in, outputStream)
  }
}
