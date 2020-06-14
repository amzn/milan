package com.amazon.milan.compiler.flink.types

import org.apache.commons.lang.builder.HashCodeBuilder
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot, TypeSerializerSnapshotSerializationUtil}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}


object OptionTypeInformation {
  def wrap[T](valueTypeInformation: TypeInformation[T]): OptionTypeInformation[T] =
    new OptionTypeInformation[T](valueTypeInformation)
}


class OptionTypeInformation[T](val valueTypeInformation: TypeInformation[T])
  extends TypeInformation[Option[T]] {

  override def isBasicType: Boolean = false

  override def isTupleType: Boolean = false

  override def getArity: Int = 1

  override def getTotalFields: Int = 1 + this.valueTypeInformation.getTotalFields

  override def getTypeClass: Class[Option[T]] = classOf[Option[T]]

  override def isKeyType: Boolean = true

  override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[Option[T]] =
    new OptionTypeSerializer[T](this.valueTypeInformation.createSerializer(executionConfig))

  override def canEqual(o: Any): Boolean = o.isInstanceOf[OptionTypeInformation[T]]

  override def toString: String = s"OptionTypeInformation(${this.valueTypeInformation})"

  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(obj: Any): Boolean = obj match {
    case o: OptionTypeInformation[T] => this.valueTypeInformation.equals(o.valueTypeInformation)
    case _ => false
  }
}


object OptionTypeSerializer {

  class Snapshot[T](private var valueSerializer: TypeSerializer[T],
                    private var valueSnapshot: TypeSerializerSnapshot[T])
    extends TypeSerializerSnapshot[Option[T]] {

    override def getCurrentVersion: Int = 1

    override def writeSnapshot(output: DataOutputView): Unit = {
      TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(output, this.valueSnapshot, this.valueSerializer)
    }

    override def readSnapshot(snapshotVersion: Int, input: DataInputView, classLoader: ClassLoader): Unit = {
      this.valueSnapshot = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(input, classLoader, null)
    }

    override def restoreSerializer(): TypeSerializer[Option[T]] = {
      this.valueSerializer = this.valueSnapshot.restoreSerializer()
      new OptionTypeSerializer[T](this.valueSerializer)
    }

    override def resolveSchemaCompatibility(typeSerializer: TypeSerializer[Option[T]]): TypeSerializerSchemaCompatibility[Option[T]] = {
      val valueResult = this.valueSnapshot.resolveSchemaCompatibility(this.valueSerializer)

      if (valueResult.isCompatibleAfterMigration) {
        TypeSerializerSchemaCompatibility.compatibleAfterMigration()
      }
      else if (valueResult.isCompatibleAsIs) {
        TypeSerializerSchemaCompatibility.compatibleAsIs()
      }
      else if (valueResult.isCompatibleWithReconfiguredSerializer) {
        TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(new OptionTypeSerializer[T](valueResult.getReconfiguredSerializer))
      }
      else {
        TypeSerializerSchemaCompatibility.incompatible()
      }
    }
  }

}


class OptionTypeSerializer[T](val valueTypeSerializer: TypeSerializer[T])
  extends TypeSerializer[Option[T]] {

  override def isImmutableType: Boolean = true

  override def duplicate(): TypeSerializer[Option[T]] = this

  override def createInstance(): Option[T] = None

  override def copy(value: Option[T]): Option[T] = value

  override def copy(value: Option[T], dest: Option[T]): Option[T] = value

  override def getLength: Int = 0

  override def serialize(value: Option[T], output: DataOutputView): Unit = {
    value match {
      case Some(inner) =>
        output.writeBoolean(true)
        this.valueTypeSerializer.serialize(inner, output)

      case None =>
        output.writeBoolean(false)
    }
  }

  override def deserialize(input: DataInputView): Option[T] = {
    if (input.readBoolean()) {
      Some(this.valueTypeSerializer.deserialize(input))
    }
    else {
      None
    }
  }

  override def deserialize(dest: Option[T], input: DataInputView): Option[T] = {
    this.deserialize(input)
  }

  override def copy(input: DataInputView, output: DataOutputView): Unit = {
    if (input.readBoolean()) {
      output.writeBoolean(true)
      this.valueTypeSerializer.copy(input, output)
    }
    else {
      output.writeBoolean(false)
    }
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[Option[T]] = {
    new OptionTypeSerializer.Snapshot[T](this.valueTypeSerializer, this.valueTypeSerializer.snapshotConfiguration())
  }

  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(obj: Any): Boolean = obj match {
    case o: OptionTypeSerializer[T] => this.valueTypeSerializer.equals(o.valueTypeSerializer)
    case _ => false
  }
}
