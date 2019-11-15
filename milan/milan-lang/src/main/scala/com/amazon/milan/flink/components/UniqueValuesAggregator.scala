package com.amazon.milan.flink.components

import java.io.ByteArrayOutputStream
import java.util

import com.amazon.milan.flink.api.MilanAggregateFunction
import com.amazon.milan.flink.compiler.internal.{SerializableFunction, SerializableFunction2}
import com.amazon.milan.flink.components.UniqueValuesAccumulatorSerializer.Snapshot
import org.apache.commons.lang.builder.HashCodeBuilder
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView, DataOutputViewStreamWrapper}

import scala.collection.JavaConverters._


class UniqueValuesAccumulator[T, TKey](val values: util.HashMap[TKey, T]) extends Serializable {

}


class UniqueValuesAggregator[T, TKey, TResult](keyExtractor: SerializableFunction[T, TKey],
                                               collisionResolver: SerializableFunction2[T, T, T],
                                               outputFunction: SerializableFunction[Iterable[T], TResult],
                                               inputTypeInformation: TypeInformation[T],
                                               keyTypeInformation: TypeInformation[TKey],
                                               outputTypeInformation: TypeInformation[TResult])
  extends MilanAggregateFunction[T, UniqueValuesAccumulator[T, TKey], TResult] {

  override def createAccumulator(): UniqueValuesAccumulator[T, TKey] =
    new UniqueValuesAccumulator[T, TKey](new util.HashMap[TKey, T]())

  override def add(in: T, acc: UniqueValuesAccumulator[T, TKey]): UniqueValuesAccumulator[T, TKey] = {
    val key = this.keyExtractor(in)
    val newValues = new util.HashMap[TKey, T](acc.values)

    if (acc.values.containsKey(key)) {
      val currentValue = acc.values.get(key)
      val resolvedValue = this.collisionResolver(currentValue, in)
      newValues.put(key, resolvedValue)
    }
    else {
      newValues.put(key, in)
    }

    new UniqueValuesAccumulator[T, TKey](newValues)
  }

  override def getResult(acc: UniqueValuesAccumulator[T, TKey]): TResult = {
    this.outputFunction(acc.values.values().asScala)
  }

  override def merge(acc: UniqueValuesAccumulator[T, TKey],
                     acc1: UniqueValuesAccumulator[T, TKey]): UniqueValuesAccumulator[T, TKey] = {
    val mergedValues = this.mergeMaps(acc.values, acc1.values)
    new UniqueValuesAccumulator[T, TKey](mergedValues)
  }

  override def getAccumulatorType: TypeInformation[UniqueValuesAccumulator[T, TKey]] = {
    new UniqueValuesAccumulatorTypeInformation[T, TKey](this.inputTypeInformation, this.keyTypeInformation)
  }

  override def getProducedType: TypeInformation[TResult] = this.outputTypeInformation

  private def mergeMaps(map1: util.HashMap[TKey, T], map2: util.HashMap[TKey, T]): util.HashMap[TKey, T] = {
    val merged = new util.HashMap[TKey, T](map1)
    map2.forEach((key: TKey, value: T) => merged.merge(key, value, (currentValue, newValue) => this.collisionResolver(currentValue, newValue)))
    merged
  }
}


class UniqueValuesAccumulatorTypeInformation[T, TKey](val valueTypeInformation: TypeInformation[T],
                                                      val keyTypeInformation: TypeInformation[TKey]) extends TypeInformation[UniqueValuesAccumulator[T, TKey]] {
  override def getGenericParameters: util.Map[String, TypeInformation[_]] = {
    val map = new util.HashMap[String, TypeInformation[_]](2)
    map.put("T", valueTypeInformation)
    map.put("TKey", keyTypeInformation)
    map
  }

  override def isBasicType: Boolean = false

  override def isTupleType: Boolean = false

  override def getArity: Int = 1

  override def getTotalFields: Int = 1

  override def getTypeClass: Class[UniqueValuesAccumulator[T, TKey]] =
    classOf[UniqueValuesAccumulator[T, TKey]]

  override def isKeyType: Boolean = false

  override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[UniqueValuesAccumulator[T, TKey]] = {
    val valueSerializer = this.valueTypeInformation.createSerializer(executionConfig)
    val keySerializer = this.keyTypeInformation.createSerializer(executionConfig)
    new UniqueValuesAccumulatorSerializer[T, TKey](valueSerializer, keySerializer)
  }

  override def canEqual(o: Any): Boolean = o.isInstanceOf[UniqueValuesAccumulatorTypeInformation[T, TKey]]

  override def toString: String = s"UniqueValuesAccumulatorTypeInformation[$valueTypeInformation, $keyTypeInformation]"

  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(obj: Any): Boolean = obj match {
    case o: UniqueValuesAccumulatorTypeInformation[T, TKey] =>
      this.valueTypeInformation.equals(o.valueTypeInformation) &&
        this.keyTypeInformation.equals(o.keyTypeInformation)

    case _ =>
      false
  }
}


object UniqueValuesAccumulatorSerializer {

  class Snapshot[T, TKey](valueSerializerSnapshot: TypeSerializerSnapshot[T],
                          keySerializerSnapshot: TypeSerializerSnapshot[TKey])
    extends TypeSerializerSnapshot[UniqueValuesAccumulator[T, TKey]] {

    override def getCurrentVersion: Int = 1

    override def writeSnapshot(output: DataOutputView): Unit = {
      output.writeInt(this.valueSerializerSnapshot.getCurrentVersion)
      this.valueSerializerSnapshot.writeSnapshot(output)

      output.writeInt(this.keySerializerSnapshot.getCurrentVersion)
      this.keySerializerSnapshot.writeSnapshot(output)
    }

    override def readSnapshot(readVersion: Int, input: DataInputView, classLoader: ClassLoader): Unit = {
      val valueSnapshotVersion = input.readInt()
      this.valueSerializerSnapshot.readSnapshot(valueSnapshotVersion, input, classLoader)

      val keySnapshotVersion = input.readInt()
      this.keySerializerSnapshot.readSnapshot(keySnapshotVersion, input, classLoader)
    }

    override def restoreSerializer(): TypeSerializer[UniqueValuesAccumulator[T, TKey]] = {
      val valueSerializer = this.valueSerializerSnapshot.restoreSerializer()
      val keySerializer = this.keySerializerSnapshot.restoreSerializer()
      new UniqueValuesAccumulatorSerializer[T, TKey](valueSerializer, keySerializer)
    }

    override def resolveSchemaCompatibility(typeSerializer: TypeSerializer[UniqueValuesAccumulator[T, TKey]]): TypeSerializerSchemaCompatibility[UniqueValuesAccumulator[T, TKey]] = {
      val serializer = typeSerializer.asInstanceOf[UniqueValuesAccumulatorSerializer[T, TKey]]

      val childResults = List(
        this.valueSerializerSnapshot.resolveSchemaCompatibility(serializer.valueSerializer),
        this.keySerializerSnapshot.resolveSchemaCompatibility(serializer.keySerializer)
      )

      if (childResults.exists(_.isIncompatible)) {
        TypeSerializerSchemaCompatibility.incompatible()
      }
      else if (childResults.exists(_.isCompatibleAfterMigration)) {
        TypeSerializerSchemaCompatibility.compatibleAfterMigration()
      }
      else {
        TypeSerializerSchemaCompatibility.compatibleAsIs()
      }
    }
  }

}


class UniqueValuesAccumulatorSerializer[T, TKey](val valueSerializer: TypeSerializer[T],
                                                 val keySerializer: TypeSerializer[TKey])
  extends TypeSerializer[UniqueValuesAccumulator[T, TKey]] {

  override def isImmutableType: Boolean = true

  override def duplicate(): TypeSerializer[UniqueValuesAccumulator[T, TKey]] = this

  override def createInstance(): UniqueValuesAccumulator[T, TKey] =
    new UniqueValuesAccumulator[T, TKey](new util.HashMap[TKey, T]())

  override def copy(source: UniqueValuesAccumulator[T, TKey]): UniqueValuesAccumulator[T, TKey] = source

  override def copy(source: UniqueValuesAccumulator[T, TKey],
                    dest: UniqueValuesAccumulator[T, TKey]): UniqueValuesAccumulator[T, TKey] = source

  override def getLength: Int = 0

  override def serialize(value: UniqueValuesAccumulator[T, TKey], output: DataOutputView): Unit = {
    // First write the contents to a buffer, so that we can write the length of the buffer as the first entry
    // in the output. This makes the copy operation much simpler.
    val buffer = new ByteArrayOutputStream()
    val bufferWriter = new DataOutputViewStreamWrapper(buffer)

    bufferWriter.writeInt(value.values.size())
    value.values.forEach((k, v) => {
      this.keySerializer.serialize(k, bufferWriter)
      this.valueSerializer.serialize(v, bufferWriter)
    })

    val bytes = buffer.toByteArray
    output.writeInt(bytes.length)
    output.write(bytes)
  }

  override def deserialize(input: DataInputView): UniqueValuesAccumulator[T, TKey] = {
    // The first integer is the size of the data, which we don't need here.
    input.readInt()

    val count = input.readInt()
    val values = new util.HashMap[TKey, T](count)

    for (_ <- 1 to count) {
      val key = this.keySerializer.deserialize(input)
      val value = this.valueSerializer.deserialize(input)
      values.put(key, value)
    }

    new UniqueValuesAccumulator[T, TKey](values)
  }

  override def deserialize(dest: UniqueValuesAccumulator[T, TKey],
                           input: DataInputView): UniqueValuesAccumulator[T, TKey] = {
    this.deserialize(input)
  }

  override def copy(input: DataInputView, output: DataOutputView): Unit = {
    val length = input.readInt()
    output.writeInt(length)
    output.write(input, length)
  }

  override def canEqual(o: Any): Boolean = o.isInstanceOf[UniqueValuesAccumulatorSerializer[T, TKey]]

  override def snapshotConfiguration(): TypeSerializerSnapshot[UniqueValuesAccumulator[T, TKey]] = {
    val valueSnapshot = this.valueSerializer.snapshotConfiguration()
    val keySnapshot = this.keySerializer.snapshotConfiguration()
    new Snapshot[T, TKey](valueSnapshot, keySnapshot)
  }

  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(obj: Any): Boolean = obj match {
    case o: UniqueValuesAccumulatorSerializer[T, TKey] =>
      this.valueSerializer.equals(o.valueSerializer) &&
        this.keySerializer.equals(o.keySerializer)

    case _ =>
      false
  }
}