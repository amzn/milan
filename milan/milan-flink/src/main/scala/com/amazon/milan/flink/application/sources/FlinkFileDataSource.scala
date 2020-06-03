package com.amazon.milan.flink.application.sources

import com.amazon.milan.application.sources.FileDataSource
import com.amazon.milan.dataformats.DataInputFormat
import com.amazon.milan.flink.RuntimeEvaluator
import com.amazon.milan.flink.application.FlinkDataSource
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.typesafe.scalalogging.Logger
import org.apache.commons.lang.builder.HashCodeBuilder
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.io.{DelimitedInputFormat, FilePathFilter}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory


@JsonDeserialize
class FlinkFileDataSource[T](path: String,
                             dataFormat: DataInputFormat[T],
                             configuration: FileDataSource.Configuration,
                             private var recordTypeInformation: TypeInformation[T])
  extends FlinkDataSource[T] {

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  @JsonCreator
  def this(path: String, dataFormat: DataInputFormat[T], configuration: FileDataSource.Configuration) {
    this(path, dataFormat, configuration, null)
  }

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeInformation = RuntimeEvaluator.instance.createTypeInformation[T](genericArgs.head.asInstanceOf[TypeDescriptor[T]])
  }

  override def addDataSource(env: StreamExecutionEnvironment): SingleOutputStreamOperator[T] = {
    this.logger.info(s"Adding file '${this.path}' as an input to the streaming environment. ")

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
      this.path,
      processingMode,
      changeCheckIntervalMs,
      new ByteArrayRecordTypeInformation)

    val mapper = new ByteArrayDataFormatFlatMapFunction[T](this.dataFormat, recordTypeInformation)
    inputLines.flatMap(mapper)
  }
}


class ByteArrayRecord(val bytes: Array[Byte], val offset: Int, val length: Int) {
}


class ByteArrayRecordSerializer extends TypeSerializer[ByteArrayRecord] {
  override def getLength: Int = -1

  override def isImmutableType: Boolean = true

  override def duplicate(): TypeSerializer[ByteArrayRecord] = this

  override def createInstance(): ByteArrayRecord = new ByteArrayRecord(Array.emptyByteArray, 0, 0)

  override def copy(source: ByteArrayRecord): ByteArrayRecord = new ByteArrayRecord(source.bytes, source.offset, source.length)

  override def copy(source: ByteArrayRecord, dest: ByteArrayRecord): ByteArrayRecord = new ByteArrayRecord(source.bytes, source.offset, source.length)

  override def serialize(value: ByteArrayRecord, output: DataOutputView): Unit = {
    output.writeInt(value.length)
    output.write(value.bytes, value.offset, value.length)
  }

  override def deserialize(input: DataInputView): ByteArrayRecord = {
    val length = input.readInt()
    val bytes = Array.ofDim[Byte](length)
    input.read(bytes)
    new ByteArrayRecord(bytes, 0, length)
  }

  override def deserialize(dest: ByteArrayRecord, input: DataInputView): ByteArrayRecord = {
    this.deserialize(input)
  }

  override def copy(input: DataInputView, output: DataOutputView): Unit = {
    val length = input.readInt()
    output.writeInt(length)
    output.write(input, length)
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[ByteArrayRecord] = new ByteArrayRecordSerializerSnapshot

  override def equals(obj: Any): Boolean = obj.isInstanceOf[ByteArrayRecordSerializer]

  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)
}

class ByteArrayRecordSerializerSnapshot
  extends TypeSerializerSnapshot[ByteArrayRecord]
    with Serializable {

  override def getCurrentVersion: Int = 1

  override def writeSnapshot(dataOutputView: DataOutputView): Unit = {}

  override def readSnapshot(i: Int, dataInputView: DataInputView, classLoader: ClassLoader): Unit = {}

  override def restoreSerializer(): TypeSerializer[ByteArrayRecord] = new ByteArrayRecordSerializer

  override def resolveSchemaCompatibility(typeSerializer: TypeSerializer[ByteArrayRecord]): TypeSerializerSchemaCompatibility[ByteArrayRecord] = {
    TypeSerializerSchemaCompatibility.compatibleAsIs()
  }
}

class ByteArrayRecordTypeInformation extends TypeInformation[ByteArrayRecord] {
  override def isBasicType: Boolean = false

  override def isTupleType: Boolean = false

  override def getArity: Int = 1

  override def getTotalFields: Int = 1

  override def getTypeClass: Class[ByteArrayRecord] = classOf[ByteArrayRecord]

  override def isKeyType: Boolean = false

  override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[ByteArrayRecord] =
    new ByteArrayRecordSerializer

  override def canEqual(o: Any): Boolean = o.isInstanceOf[ByteArrayRecordTypeInformation]

  override def toString: String = "ByteArrayRecord"

  override def equals(obj: Any): Boolean = obj.isInstanceOf[ByteArrayRecordTypeInformation]

  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)
}


class ByteArrayInputFormat extends DelimitedInputFormat[ByteArrayRecord] {
  override def readRecord(value: ByteArrayRecord, bytes: Array[Byte], offset: Int, length: Int): ByteArrayRecord = {
    new ByteArrayRecord(bytes, offset, length)
  }
}


class ByteArrayDataFormatFlatMapFunction[T](dataFormat: DataInputFormat[T], outputTypeInfo: TypeInformation[T])
  extends FlatMapFunction[ByteArrayRecord, T]
    with ResultTypeQueryable[T] {

  override def flatMap(record: ByteArrayRecord, collector: Collector[T]): Unit = {
    dataFormat.readValue(record.bytes, record.offset, record.length) match {
      case Some(value) => collector.collect(value)
      case None => ()
    }
  }

  override def getProducedType: TypeInformation[T] = this.outputTypeInfo
}
