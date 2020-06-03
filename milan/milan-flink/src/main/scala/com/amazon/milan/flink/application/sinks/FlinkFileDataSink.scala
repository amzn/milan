package com.amazon.milan.flink.application.sinks

import java.io.OutputStream
import java.nio.file.{Files, Paths, StandardOpenOption}

import com.amazon.milan.dataformats.DataOutputFormat
import com.amazon.milan.flink.application.FlinkDataSink
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import org.apache.flink.streaming.api.functions.sink.SinkFunction


@JsonSerialize
@JsonDeserialize
class FlinkFileDataSink[T](path: String, outputFormat: DataOutputFormat[T]) extends FlinkDataSink[T] {
  override def getSinkFunction: SinkFunction[T] = {
    new FileSinkFunction[T](this.path, this.outputFormat)
  }

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
  }

  override def getMaxParallelism: Option[Int] = Some(1)
}


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
