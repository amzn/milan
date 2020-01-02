package com.amazon.milan.flink.components

import com.amazon.milan.types.{LineageRecord, RecordWithLineage}
import org.apache.commons.lang.builder.HashCodeBuilder
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.memory.{DataInputView, DataOutputView}


object RecordWithLineageSerializer {

  class ConfigSnapshot[T](val recordConfigSnapshot: TypeSerializerSnapshot[T],
                          val lineageConfigSnapshot: TypeSerializerSnapshot[LineageRecord])
    extends TypeSerializerSnapshot[RecordWithLineage[T]]
      with Serializable {

    override def getCurrentVersion: Int =
      this.recordConfigSnapshot.getCurrentVersion

    override def writeSnapshot(output: DataOutputView): Unit = {
      this.recordConfigSnapshot.writeSnapshot(output)

      output.writeInt(this.lineageConfigSnapshot.getCurrentVersion)
      this.lineageConfigSnapshot.writeSnapshot(output)
    }

    override def readSnapshot(readVersion: Int, input: DataInputView, classLoader: ClassLoader): Unit = {
      this.recordConfigSnapshot.readSnapshot(readVersion, input, classLoader)

      val lineageVersion = input.readInt()
      this.lineageConfigSnapshot.readSnapshot(lineageVersion, input, classLoader)
    }

    override def restoreSerializer(): TypeSerializer[RecordWithLineage[T]] = {
      val recordSerializer = this.recordConfigSnapshot.restoreSerializer()
      val lineageRecordSerializer = this.lineageConfigSnapshot.restoreSerializer()
      new RecordWithLineageSerializer[T](recordSerializer, lineageRecordSerializer)
    }

    override def resolveSchemaCompatibility(typeSerializer: TypeSerializer[RecordWithLineage[T]]): TypeSerializerSchemaCompatibility[RecordWithLineage[T]] = {
      val serializer = typeSerializer.asInstanceOf[RecordWithLineageSerializer[T]]
      val recordResult = this.recordConfigSnapshot.resolveSchemaCompatibility(serializer.recordSerializer)
      val lineageResult = this.lineageConfigSnapshot.resolveSchemaCompatibility(serializer.lineageSerializer)

      if (recordResult.isIncompatible || lineageResult.isIncompatible) {
        TypeSerializerSchemaCompatibility.incompatible()
      }
      else if (recordResult.isCompatibleAfterMigration || lineageResult.isCompatibleAfterMigration) {
        TypeSerializerSchemaCompatibility.compatibleAfterMigration()
      }
      else {
        TypeSerializerSchemaCompatibility.compatibleAsIs()
      }
    }

    override def equals(obj: Any): Boolean = obj match {
      case o: ConfigSnapshot[T] =>
        this.recordConfigSnapshot.equals(o.recordConfigSnapshot) &&
          this.lineageConfigSnapshot.equals(o.lineageConfigSnapshot)

      case _ =>
        false
    }

    override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)
  }

}

import com.amazon.milan.flink.components.RecordWithLineageSerializer._


class RecordWithLineageSerializer[T](val recordSerializer: TypeSerializer[T],
                                     val lineageSerializer: TypeSerializer[LineageRecord])
  extends TypeSerializer[RecordWithLineage[T]] {

  def this(recordSerializer: TypeSerializer[T], executionConfig: ExecutionConfig) {
    this(recordSerializer, createTypeInformation[LineageRecord].createSerializer(executionConfig))
  }

  override def canEqual(o: Any): Boolean = o.isInstanceOf[RecordWithLineageSerializer[T]]

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    this.recordSerializer.copy(source, target)
    this.lineageSerializer.copy(source, target)
  }

  override def copy(item: RecordWithLineage[T]): RecordWithLineage[T] = {
    val recordCopy = this.recordSerializer.copy(item.record)
    val sourceCopy = this.lineageSerializer.copy(item.lineage)
    RecordWithLineage[T](recordCopy, sourceCopy)
  }

  override def copy(source: RecordWithLineage[T], target: RecordWithLineage[T]): RecordWithLineage[T] = {
    this.copy(source)
  }

  override def createInstance(): RecordWithLineage[T] = {
    RecordWithLineage[T](this.recordSerializer.createInstance(), null)
  }

  override def deserialize(source: DataInputView): RecordWithLineage[T] = {
    val record = this.recordSerializer.deserialize(source)

    val hasLineage = source.readBoolean()
    if (hasLineage) {
      val lineage = this.lineageSerializer.deserialize(source)
      RecordWithLineage[T](record, lineage)
    }
    else {
      RecordWithLineage[T](record, null)
    }
  }

  override def deserialize(target: RecordWithLineage[T], source: DataInputView): RecordWithLineage[T] = {
    this.deserialize(source)
  }

  override def duplicate(): TypeSerializer[RecordWithLineage[T]] = {
    new RecordWithLineageSerializer[T](this.recordSerializer.duplicate(), this.lineageSerializer.duplicate())
  }

  override def getLength: Int = -1

  override def isImmutableType: Boolean = true

  override def serialize(item: RecordWithLineage[T], dataOutputView: DataOutputView): Unit = {
    this.recordSerializer.serialize(item.record, dataOutputView)

    if (item.lineage == null) {
      dataOutputView.writeBoolean(false)
    }
    else {
      dataOutputView.writeBoolean(true)
      this.lineageSerializer.serialize(item.lineage, dataOutputView)
    }
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[RecordWithLineage[T]] = {
    val recordSnapshot = this.recordSerializer.snapshotConfiguration()
    val lineageSnapshot = this.lineageSerializer.snapshotConfiguration()
    new ConfigSnapshot[T](recordSnapshot, lineageSnapshot)
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: RecordWithLineageSerializer[T] =>
      this.recordSerializer.equals(o.recordSerializer) &&
        this.lineageSerializer.equals(o.lineageSerializer)

    case _ =>
      false
  }

  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)
}
