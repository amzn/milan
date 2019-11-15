package com.amazon.milan.flink.application.sources

import com.amazon.milan.dataformats.DataFormat
import com.amazon.milan.flink.RuntimeEvaluator
import com.amazon.milan.flink.application.FlinkDataSource
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.slf4j.LoggerFactory


@JsonDeserialize
class FlinkFileDataSource[T](path: String,
                             dataFormat: DataFormat[T],
                             var recordTypeInformation: TypeInformation[T])
  extends FlinkDataSource[T] {

  @transient private val logger = Logger(LoggerFactory.getLogger(getClass))

  @JsonCreator
  def this(path: String, dataFormat: DataFormat[T]) {
    this(path, dataFormat, null)
  }

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeInformation = RuntimeEvaluator.instance.createTypeInformation[T](genericArgs.head.asInstanceOf[TypeDescriptor[T]])
  }

  override def addDataSource(env: StreamExecutionEnvironment): DataStreamSource[T] = {
    this.logger.info(s"Adding file '${this.path}' as an input to the streaming environment. ")

    val fileFormat = new DataFormatInputFormat[T](this.dataFormat)

    env.readFile(fileFormat, this.path, FileProcessingMode.PROCESS_ONCE, 0, this.recordTypeInformation)
  }
}
