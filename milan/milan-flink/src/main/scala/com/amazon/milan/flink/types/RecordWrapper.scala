package com.amazon.milan.flink.types

import java.util

import com.amazon.milan.flink.types.RecordWrapperTypeSerializer.Snapshot
import org.apache.commons.lang.builder.HashCodeBuilder
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot, TypeSerializerSnapshotSerializationUtil}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}


object RecordWrapper {
  def wrap[T >: Null, TKey >: Null <: Product](value: T, key: TKey, sequenceNumber: Long): RecordWrapper[T, TKey] =
    new RecordWrapper[T, TKey](value, key, sequenceNumber)

  def wrap[T >: Null](value: T, sequenceNumber: Long): RecordWrapper[T, Product] =
    new RecordWrapper[T, Product](value, None, sequenceNumber)
}


class RecordWrapper[T >: Null, TKey >: Null <: Product](var value: T,
                                                        var key: TKey,
                                                        var sequenceNumber: Long) extends Serializable {
  def this() {
    this(null, null, 0)
  }

  def withSequenceNumber(sequenceNumber: Long): RecordWrapper[T, TKey] =
    new RecordWrapper[T, TKey](this.value, this.key, sequenceNumber)

  def withKey[TNewKey >: Null <: Product](newKey: TNewKey): RecordWrapper[T, TNewKey] =
    new RecordWrapper[T, TNewKey](this.value, newKey, this.sequenceNumber)

  override def toString: String = s"($value, $key)"

  override def equals(obj: Any): Boolean = obj match {
    case r: RecordWrapper[T, TKey] => this.value.equals(r.value) && this.key.equals(r.key)
    case _ => false
  }
}


object RecordWrapperTypeInformation {
  def wrap[T >: Null, TKey >: Null <: Product](valueTypeInformation: TypeInformation[T],
                                               keyTypeInformation: TypeInformation[TKey]): RecordWrapperTypeInformation[T, TKey] =
    new RecordWrapperTypeInformation[T, TKey](valueTypeInformation, keyTypeInformation)

  def wrap[T >: Null](valueTypeInformation: TypeInformation[T]): RecordWrapperTypeInformation[T, Product] =
    new RecordWrapperTypeInformation[T, Product](valueTypeInformation, new NoneTypeInformation())
}


class RecordWrapperTypeInformation[T >: Null, TKey >: Null <: Product](val valueTypeInformation: TypeInformation[T],
                                                                       val keyTypeInformation: TypeInformation[TKey])
  extends TypeInformation[RecordWrapper[T, TKey]] {

  override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[RecordWrapper[T, TKey]] = {
    new RecordWrapperTypeSerializer[T, TKey](
      valueTypeInformation.createSerializer(executionConfig),
      keyTypeInformation.createSerializer(executionConfig))
  }

  override def getGenericParameters: util.Map[String, TypeInformation[_]] = {
    val map = new util.HashMap[String, TypeInformation[_]]()
    map.put("T", this.valueTypeInformation)
    map.put("TKey", this.keyTypeInformation)
    map
  }

  override def getArity: Int = 2

  override def getTotalFields: Int = this.getArity + this.valueTypeInformation.getTotalFields

  override def getTypeClass: Class[RecordWrapper[T, TKey]] = classOf[RecordWrapper[T, TKey]]

  override def isBasicType: Boolean = false

  override def isKeyType: Boolean = true

  override def isSortKeyType: Boolean = false

  override def isTupleType: Boolean = false

  override def canEqual(o: Any): Boolean = {
    o match {
      case _: RecordWrapperTypeInformation[T, TKey] =>
        true

      case _ =>
        false
    }
  }

  override def toString: String = s"RecordWrapper(${this.valueTypeInformation}, ${this.keyTypeInformation})"

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: RecordWrapperTypeInformation[T, TKey] =>
        this.valueTypeInformation.equals(o.valueTypeInformation) &&
          this.keyTypeInformation.equals(o.keyTypeInformation)

      case _ =>
        false
    }
  }

  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)
}


object RecordWrapperTypeSerializer {

  class Snapshot[T >: Null, TKey >: Null <: Product](private var valueSnapshot: TypeSerializerSnapshot[T],
                                                     private var valueSerializer: TypeSerializer[T],
                                                     private var keySnapshot: TypeSerializerSnapshot[TKey],
                                                     private var keySerializer: TypeSerializer[TKey])
    extends TypeSerializerSnapshot[RecordWrapper[T, TKey]] {

    def this() {
      this(null, null, null, null)
    }

    override def getCurrentVersion: Int = 0

    override def writeSnapshot(output: DataOutputView): Unit = {
      TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(output, this.valueSnapshot, this.valueSerializer)
      TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(output, this.keySnapshot, this.keySerializer)
    }

    override def readSnapshot(snapshotVersion: Int, input: DataInputView, classLoader: ClassLoader): Unit = {
      this.valueSnapshot = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(input, classLoader, null)
      this.keySnapshot = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(input, classLoader, null)
    }

    override def restoreSerializer(): TypeSerializer[RecordWrapper[T, TKey]] = {
      this.valueSerializer = this.valueSnapshot.restoreSerializer()
      this.keySerializer = this.keySnapshot.restoreSerializer()
      new RecordWrapperTypeSerializer[T, TKey](this.valueSerializer, this.keySerializer)
    }

    override def resolveSchemaCompatibility(typeSerializer: TypeSerializer[RecordWrapper[T, TKey]]): TypeSerializerSchemaCompatibility[RecordWrapper[T, TKey]] = {
      val resolvingValueSerializer = typeSerializer.asInstanceOf[RecordWrapperTypeSerializer[T, TKey]].valueSerializer
      val resolvingKeySerializer = typeSerializer.asInstanceOf[RecordWrapperTypeSerializer[T, TKey]].keySerializer
      SnapshotCompatibility.resolveCompatibility(
        reconfigured => new RecordWrapperTypeSerializer[T, TKey](reconfigured.head.asInstanceOf[TypeSerializer[T]], reconfigured.last.asInstanceOf[TypeSerializer[TKey]]),
        (resolvingValueSerializer, this.valueSnapshot.resolveSchemaCompatibility(resolvingValueSerializer)),
        (resolvingKeySerializer, this.keySnapshot.resolveSchemaCompatibility(resolvingKeySerializer))
      )
    }
  }

}

class RecordWrapperTypeSerializer[T >: Null, TKey >: Null <: Product](val valueSerializer: TypeSerializer[T],
                                                                      val keySerializer: TypeSerializer[TKey])
  extends TypeSerializer[RecordWrapper[T, TKey]] {

  override def isImmutableType: Boolean = false

  override def duplicate(): TypeSerializer[RecordWrapper[T, TKey]] = this

  override def createInstance(): RecordWrapper[T, TKey] =
    new RecordWrapper[T, TKey]()

  override def copy(source: RecordWrapper[T, TKey]): RecordWrapper[T, TKey] =
    new RecordWrapper[T, TKey](
      this.valueSerializer.copy(source.value),
      this.keySerializer.copy(source.key),
      source.sequenceNumber)

  override def copy(source: RecordWrapper[T, TKey], dest: RecordWrapper[T, TKey]): RecordWrapper[T, TKey] = {
    dest.sequenceNumber = source.sequenceNumber
    dest.value = this.valueSerializer.copy(source.value)
    dest.key = this.keySerializer.copy(source.key)
    dest
  }

  override def copy(input: DataInputView, output: DataOutputView): Unit = {
    output.writeLong(input.readLong())
    this.valueSerializer.copy(input, output)
    this.keySerializer.copy(input, output)
  }

  override def getLength: Int = -1

  override def serialize(record: RecordWrapper[T, TKey], output: DataOutputView): Unit = {
    output.writeLong(record.sequenceNumber)
    this.valueSerializer.serialize(record.value, output)
    this.keySerializer.serialize(record.key, output)
  }

  override def deserialize(input: DataInputView): RecordWrapper[T, TKey] = {
    val sequenceNumber = input.readLong()
    val value = this.valueSerializer.deserialize(input)
    val key = this.keySerializer.deserialize(input)
    new RecordWrapper[T, TKey](value, key, sequenceNumber)
  }

  override def deserialize(dest: RecordWrapper[T, TKey], input: DataInputView): RecordWrapper[T, TKey] = {
    dest.sequenceNumber = input.readLong()
    dest.value = this.valueSerializer.deserialize(input)
    dest.key = this.keySerializer.deserialize(input)
    dest
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[RecordWrapper[T, TKey]] = {
    new Snapshot[T, TKey](
      this.valueSerializer.snapshotConfiguration(),
      this.valueSerializer,
      this.keySerializer.snapshotConfiguration(),
      this.keySerializer)
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: RecordWrapperTypeSerializer[T, TKey] =>
      this.valueSerializer.equals(o.valueSerializer) &&
        this.keySerializer.equals(o.keySerializer)

    case _ =>
      false
  }

  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)
}
