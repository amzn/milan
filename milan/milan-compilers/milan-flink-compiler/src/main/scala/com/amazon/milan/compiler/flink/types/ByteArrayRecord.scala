package com.amazon.milan.compiler.flink.types

import com.amazon.milan.dataformats.DataInputFormat
import org.apache.commons.lang.builder.HashCodeBuilder
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.io.DelimitedInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.Collector


class ByteArrayRecord(var bytes: Array[Byte], var offset: Int, var length: Int) extends Serializable {
  def this() {
    this(Array.empty, 0, 0)
  }
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

