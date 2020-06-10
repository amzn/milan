package com.amazon.milan.flink.runtime

import java.io.OutputStream
import java.nio.file.{Files, Paths, StandardOpenOption}

import com.amazon.milan.dataformats.DataOutputFormat
import org.apache.flink.streaming.api.functions.sink.SinkFunction


class FileSinkFunction[T](path: String, dataFormat: DataOutputFormat[T]) extends SinkFunction[T] {
  @transient private lazy val outputStream = this.openOutputStream()

  override def invoke(value: T, context: SinkFunction.Context[_]): Unit = {
    this.dataFormat.writeValue(value, this.outputStream)
    this.outputStream.flush()
  }

  private def openOutputStream(): OutputStream = {
    Files.newOutputStream(Paths.get(this.path), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }
}
