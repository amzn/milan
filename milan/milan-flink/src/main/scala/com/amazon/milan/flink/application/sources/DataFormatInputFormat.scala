package com.amazon.milan.flink.application.sources

import com.amazon.milan.dataformats.DataFormat
import org.apache.flink.api.common.io.DelimitedInputFormat


/**
 * An implmentation of [[DelimitedInputFormat]] that uses a Milan [[DataFormat]] to read data.
 *
 * @param dataFormat A [[DataFormat]] used to read data.
 * @tparam T The record type.
 */
class DataFormatInputFormat[T](dataFormat: DataFormat[T]) extends DelimitedInputFormat[T] {
  override def readRecord(value: T, bytes: Array[Byte], offset: Int, length: Int): T = {
    this.dataFormat.readValue(bytes, offset, length)
  }
}
