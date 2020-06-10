package com.amazon.milan.flink.runtime

import com.amazon.milan.application.sources.FileDataSource
import com.amazon.milan.dataformats.DataInputFormat
import com.amazon.milan.flink.types.{ByteArrayDataFormatFlatMapFunction, ByteArrayInputFormat, ByteArrayRecordTypeInformation}
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


object DataSourceUtil {
  private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  def addFileDataSource[T](env: StreamExecutionEnvironment,
                           path: String,
                           dataFormat: DataInputFormat[T],
                           configuration: FileDataSource.Configuration,
                           recordTypeInformation: TypeInformation[T]): SingleOutputStreamOperator[T] = {
    this.logger.info(s"Adding file '$path' as an input to the streaming environment. ")

    val inputFormat = new ByteArrayInputFormat
    inputFormat.setFilesFilter(FilePathFilter.createDefaultFilter())

    val processingMode = configuration.readMode match {
      case FileDataSource.ReadMode.Continuous => FileProcessingMode.PROCESS_CONTINUOUSLY
      case FileDataSource.ReadMode.Once => FileProcessingMode.PROCESS_ONCE
    }

    val changeCheckIntervalMs = processingMode match {
      case FileProcessingMode.PROCESS_CONTINUOUSLY => 5000L
      case _ => -1L
    }

    val inputLines = env.readFile(
      inputFormat,
      path,
      processingMode,
      changeCheckIntervalMs,
      new ByteArrayRecordTypeInformation)

    val mapper = new ByteArrayDataFormatFlatMapFunction[T](dataFormat, recordTypeInformation)
    inputLines.flatMap(mapper)
  }

  def addListDataSource[T](env: StreamExecutionEnvironment,
                           values: List[T],
                           runForever: Boolean,
                           recordTypeInformation: TypeInformation[T]): DataStreamSource[T] = {
    if (runForever) {
      // If we don't want the source to terminate after the elements run out then we need to use a custom source
      // function rather than env.fromCollection. In order to not cause duplicate records to be sent from multiple
      // copies of the source function we set the parallelism to 1.
      val source = new ListSourceFunction[T](values, runForever)
      env.addSource(source, recordTypeInformation).setParallelism(1)
    }
    else {
      env.fromCollection(values.asJavaCollection, recordTypeInformation)
    }
  }
}
