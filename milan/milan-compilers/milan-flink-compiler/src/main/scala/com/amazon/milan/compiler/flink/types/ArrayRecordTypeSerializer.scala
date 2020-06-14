package com.amazon.milan.compiler.flink.types

import java.io.ByteArrayOutputStream

import com.amazon.milan.HashUtil
import com.amazon.milan.compiler.flink.types
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot, TypeSerializerSnapshotSerializationUtil}
import org.apache.flink.core.memory.{DataInputView, DataOutputView, DataOutputViewStreamWrapper}
import org.slf4j.LoggerFactory


object ArrayRecordTypeSerializer {

  case class FieldSnapshot[T](name: String, serializer: TypeSerializer[T], snapshot: TypeSerializerSnapshot[T])

  class Snapshot(private var fields: List[FieldSnapshot[_]])
    extends TypeSerializerSnapshot[ArrayRecord]
      with Serializable {

    /**
     * Don't use this constructor, it's here to make the class serializable.
     */
    def this() {
      this(List.empty)
    }

    override def getCurrentVersion: Int = 1

    override def readSnapshot(readVersion: Int, input: DataInputView, classLoader: ClassLoader): Unit = {
      val snapshotCount = input.readInt()
      val snapshotFields = List.tabulate(snapshotCount)(_ => input.readUTF())

      val fieldSnapshots = List.tabulate[TypeSerializerSnapshot[_]](snapshotCount)(_ =>
        TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(input, classLoader, null)
      )

      this.fields = snapshotFields.zip(fieldSnapshots).map {
        case (name, snapshot) => FieldSnapshot(name, null, snapshot)
      }
    }

    override def resolveSchemaCompatibility(typeSerializer: TypeSerializer[ArrayRecord]): TypeSerializerSchemaCompatibility[ArrayRecord] = {
      val tupleStreamTypeSerializer = typeSerializer.asInstanceOf[ArrayRecordTypeSerializer]
      val fieldResults =
        this.fields.zip(tupleStreamTypeSerializer.fieldSerializers)
          .map { case (field: FieldSnapshot[_], serializer) => this.callResolveSchemaCompatibility(field.snapshot, serializer.serializer.getInnerSerializer) }

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
      val fieldTypeSerializers = this.fields.map(_.snapshot.restoreSerializer())

      val fieldSerializers =
        this.fields
          .zip(fieldTypeSerializers)
          .map { case (field, serializer) => FieldSerializer(field.name, new TypeSerializerAnyWrapper(serializer)) }
          .toArray

      new ArrayRecordTypeSerializer(fieldSerializers)
    }

    override def writeSnapshot(output: DataOutputView): Unit = {
      output.writeInt(this.fields.length)
      this.fields.map(_.name).foreach(output.writeUTF)

      this.fields.map(_.asInstanceOf[FieldSnapshot[Any]]).foreach(field =>
        TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(output, field.snapshot, field.serializer))
    }

    private def callResolveSchemaCompatibility[T](snapshot: TypeSerializerSnapshot[T],
                                                  serializer: TypeSerializer[_]): TypeSerializerSchemaCompatibility[T] = {
      snapshot.resolveSchemaCompatibility(serializer.asInstanceOf[TypeSerializer[T]])
    }
  }

}


class ArrayRecordTypeSerializer(val fieldSerializers: Array[FieldSerializer])
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
    val fields = this.fieldSerializers
      .map(field =>
        types.ArrayRecordTypeSerializer.FieldSnapshot(
          field.fieldName,
          field.serializer.getInnerSerializer.asInstanceOf[TypeSerializer[Any]],
          field.serializer.getInnerSerializer.snapshotConfiguration().asInstanceOf[TypeSerializerSnapshot[Any]]))
      .toList
    new ArrayRecordTypeSerializer.Snapshot(fields)
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: ArrayRecordTypeSerializer =>
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

  def getInnerSerializer: TypeSerializer[_]
}


class TypeSerializerAnyWrapper[T](val innerSerializer: TypeSerializer[T])
  extends TypeSerializerAny
    with Serializable {

  def deserializeToAny(input: DataInputView): Any =
    this.innerSerializer.deserialize(input)

  def serializeAny(value: Any, output: DataOutputView): Unit =
    this.innerSerializer.serialize(value.asInstanceOf[T], output)

  override def getInnerSerializer: TypeSerializer[_] = this.innerSerializer
}


case class FieldSerializer(fieldName: String, serializer: TypeSerializerAny) extends Serializable


class DeserializationException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) {
    this(message, null)
  }
}
