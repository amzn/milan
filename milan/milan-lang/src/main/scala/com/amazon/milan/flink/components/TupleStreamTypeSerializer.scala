package com.amazon.milan.flink.components

import java.io.ByteArrayOutputStream

import com.amazon.milan.HashUtil
import com.amazon.milan.flink.components
import com.amazon.milan.flink.types.ArrayRecord
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView, DataOutputViewStreamWrapper}
import org.slf4j.LoggerFactory


object TupleStreamTypeSerializer {

  class Snapshot(fieldNames: Array[String],
                 fieldSerializerSnapshots: Array[TypeSerializerSnapshot[_]]) extends TypeSerializerSnapshot[ArrayRecord] {
    override def getCurrentVersion: Int = 1

    override def readSnapshot(readVersion: Int, input: DataInputView, classLoader: ClassLoader): Unit = {
      val snapshotCount = input.readInt()

      if (snapshotCount != fieldNames.length) {
        throw new DeserializationException(s"Wrong number of fields in snapshot. Expected ${fieldNames.length}, found $snapshotCount.")
      }

      val snapshotFields = Array.tabulate(snapshotCount)(_ => input.readUTF())
      if (!this.fieldNames.sameElements(snapshotFields)) {
        throw new DeserializationException(s"Incorrect fields in snapshot. Expected ${this.fieldNames.mkString("(", ",", ")")}, found ${snapshotFields.mkString("(", ", ", ")")}.")
      }

      this.fieldSerializerSnapshots.foreach(fieldSnapshot => {
        val snapshotVersion = input.readInt()
        fieldSnapshot.readSnapshot(snapshotVersion, input, classLoader)
      })
    }

    override def resolveSchemaCompatibility(typeSerializer: TypeSerializer[ArrayRecord]): TypeSerializerSchemaCompatibility[ArrayRecord] = {
      val tupleStreamTypeSerializer = typeSerializer.asInstanceOf[TupleStreamTypeSerializer]
      val fieldResults =
        this.fieldSerializerSnapshots.zip(tupleStreamTypeSerializer.fieldSerializers)
          .map { case (snapshot, serializer) => this.callResolveSchemaCompatibility(snapshot, serializer.serializer.getInnerSerialiazer) }

      if (fieldResults.exists(_.isIncompatible)) {
        TypeSerializerSchemaCompatibility.incompatible()
      }
      else if (fieldResults.exists(_.isCompatibleAfterMigration)) {
        TypeSerializerSchemaCompatibility.compatibleAfterMigration()
      }
      else {
        TypeSerializerSchemaCompatibility.compatibleAsIs()
      }
    }

    override def restoreSerializer(): TypeSerializer[ArrayRecord] = {
      val fieldTypeSerializers = this.fieldSerializerSnapshots.map(_.restoreSerializer())
      val fieldSerializers =
        this.fieldNames
          .zip(fieldTypeSerializers)
          .map { case (name, serializer) => FieldSerializer(name, new TypeSerializerAnyWrapper(serializer)) }

      new TupleStreamTypeSerializer(fieldSerializers)
    }

    override def writeSnapshot(output: DataOutputView): Unit = {
      output.writeInt(this.fieldNames.length)
      this.fieldNames.foreach(output.writeUTF)
      this.fieldSerializerSnapshots.foreach(fieldSnapshot => {
        output.writeInt(fieldSnapshot.getCurrentVersion)
        fieldSnapshot.writeSnapshot(output)
      })
    }

    private def callResolveSchemaCompatibility[T](snapshot: TypeSerializerSnapshot[T],
                                                  serializer: TypeSerializer[_]): TypeSerializerSchemaCompatibility[T] = {
      snapshot.resolveSchemaCompatibility(serializer.asInstanceOf[TypeSerializer[T]])
    }
  }

}


class TupleStreamTypeSerializer(val fieldSerializers: Array[FieldSerializer])
  extends TypeSerializer[ArrayRecord]
    with Serializable {

  private val hashCodeValue = HashUtil.combineObjectHashCodes(this.fieldSerializers)

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  @transient private lazy val fieldSerializerMap = this.fieldSerializers.map(f => f.fieldName -> f.serializer).toMap

  def this(executionConfig: ExecutionConfig, fields: Seq[FieldTypeInformation]) {
    this(
      fields.map(field => {
        val serializer = field.typeInfo.createSerializer(executionConfig)
        FieldSerializer(field.fieldName, new TypeSerializerAnyWrapper(serializer))
      }).toArray)
  }

  override def copy(input: DataInputView, output: DataOutputView): Unit = {
    val length = input.readInt()
    output.writeInt(length)
    output.write(input, length)
  }

  override def copy(source: ArrayRecord): ArrayRecord = source.clone()

  override def copy(source: ArrayRecord, dest: ArrayRecord): ArrayRecord = source.clone()

  override def createInstance(): ArrayRecord = ArrayRecord.empty(this.fieldSerializers.length)

  override def deserialize(input: DataInputView): ArrayRecord = {
    // The first integer is the size of the record data, which we don't need here.
    input.readInt()

    val id = input.readUTF()

    // Deserialize the field values from the input stream.
    val fieldCount = input.readInt()
    val fieldValueMap = Range(0, fieldCount)
      .map(_ => deserializeNextField(input))
      .filter(_.nonEmpty)
      .map(_.get)
      .toMap

    // Create an array of the field values, in the order we expect.
    // Any missing fields will get null, and any unexpected fields will be ignored.
    val fieldValues = this.fieldSerializers.map(f => fieldValueMap.getOrElse(f.fieldName, null))

    ArrayRecord(id, fieldValues)
  }

  override def deserialize(dest: ArrayRecord, input: DataInputView): ArrayRecord = this.deserialize(input)

  override def duplicate(): TypeSerializer[ArrayRecord] = this

  override def getLength: Int = -1

  override def isImmutableType: Boolean = true

  override def serialize(value: ArrayRecord, output: DataOutputView): Unit = {
    // First write the contents to a buffer, so that we can write the length of the buffer as the first entry
    // in the output. This makes the copy operation much simpler.
    val buffer = new ByteArrayOutputStream()
    val bufferWriter = new DataOutputViewStreamWrapper(buffer)

    bufferWriter.writeUTF(value.recordId)

    bufferWriter.writeInt(this.fieldSerializers.length)

    // Serialize the field values to the output stream.
    this.fieldSerializers.zipWithIndex.foreach {
      case (field, i) => this.serializeField(value.values, i, field, bufferWriter)
    }

    val bytes = buffer.toByteArray
    output.writeInt(bytes.length)
    output.write(bytes)
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[ArrayRecord] = {
    val fieldSnapshots = this.fieldSerializers.map(_.serializer.getInnerSerialiazer.snapshotConfiguration())
    val fieldNames = this.fieldSerializers.map(_.fieldName)
    new components.TupleStreamTypeSerializer.Snapshot(fieldNames, fieldSnapshots)
  }

  override def canEqual(o: Any): Boolean = {
    o match {
      case _: TupleStreamTypeSerializer =>
        true

      case _ =>
        false
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: TupleStreamTypeSerializer =>
      this.fieldSerializers.sameElements(o.fieldSerializers)

    case _ =>
      false
  }

  override def hashCode(): Int = this.hashCodeValue

  private def deserializeNextField(input: DataInputView): Option[(String, Any)] = {
    val fieldName = input.readUTF()
    val dataLength = input.readInt()

    this.fieldSerializerMap.get(fieldName) match {
      case Some(serializer) =>
        Some(fieldName, serializer.deserializeToAny(input))

      case None =>
        this.logger.warn(s"Found unknown field '$fieldName' in input stream.")
        input.skipBytesToRead(dataLength)
        None
    }
  }

  private def serializeField(recordValues: Array[Any],
                             fieldIndex: Int,
                             field: FieldSerializer,
                             output: DataOutputView): Unit = {
    // We write the field name, then the length of the serialized data for the field, then the data.
    // This allows us to skip fields on deserialization if we don't know how to deserialize them.
    output.writeUTF(field.fieldName)

    val fieldByteStream = new ByteArrayOutputStream()
    val fieldOutputView = new DataOutputViewStreamWrapper(fieldByteStream)
    field.serializer.serializeAny(recordValues(fieldIndex), fieldOutputView)

    val fieldBytes = fieldByteStream.toByteArray
    output.writeInt(fieldBytes.length)
    output.write(fieldBytes)
  }
}


trait TypeSerializerAny {
  def deserializeToAny(input: DataInputView): Any

  def serializeAny(value: Any, output: DataOutputView): Unit

  def getInnerSerialiazer: TypeSerializer[_]
}


class TypeSerializerAnyWrapper[T](val innerSerializer: TypeSerializer[T])
  extends TypeSerializerAny
    with Serializable {

  def deserializeToAny(input: DataInputView): Any =
    this.innerSerializer.deserialize(input)

  def serializeAny(value: Any, output: DataOutputView): Unit =
    this.innerSerializer.serialize(value.asInstanceOf[T], output)

  override def getInnerSerialiazer: TypeSerializer[_] = this.innerSerializer
}


case class FieldSerializer(fieldName: String, serializer: TypeSerializerAny) extends Serializable


class DeserializationException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) {
    this(message, null)
  }
}
