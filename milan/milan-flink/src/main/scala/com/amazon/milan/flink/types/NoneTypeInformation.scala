package com.amazon.milan.flink.types

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}


object NoneTypeInformation {
  val instance = new NoneTypeInformation
}


class NoneTypeInformation extends TypeInformation[Product] {
  override def isBasicType: Boolean = false

  override def isTupleType: Boolean = false

  override def getArity: Int = 0

  override def getTotalFields: Int = 0

  override def getTypeClass: Class[Product] = None.getClass.asInstanceOf[Class[Product]]

  override def isKeyType: Boolean = false

  override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[Product] =
    NoneTypeSerializer.instance

  override def canEqual(o: Any): Boolean = o.isInstanceOf[NoneTypeInformation]

  override def toString: String = "None"

  override def equals(obj: Any): Boolean = obj.isInstanceOf[NoneTypeInformation]

  override def hashCode(): Int = 0
}


object NoneTypeSerializer {
  val instance = new NoneTypeSerializer

  class Snapshot extends TypeSerializerSnapshot[Product] {
    override def getCurrentVersion: Int = 0

    override def writeSnapshot(dataOutputView: DataOutputView): Unit = ()

    override def readSnapshot(i: Int, dataInputView: DataInputView, classLoader: ClassLoader): Unit = ()

    override def restoreSerializer(): TypeSerializer[Product] = new NoneTypeSerializer

    override def resolveSchemaCompatibility(typeSerializer: TypeSerializer[Product]): TypeSerializerSchemaCompatibility[Product] =
      TypeSerializerSchemaCompatibility.compatibleAsIs()
  }

}


class NoneTypeSerializer extends TypeSerializer[Product] {
  override def isImmutableType: Boolean = true

  override def duplicate(): TypeSerializer[Product] = this

  override def createInstance(): Product = None

  override def copy(t: Product): Product = None

  override def copy(t: Product, t1: Product): Product = None

  override def getLength: Int = 0

  override def serialize(t: Product, dataOutputView: DataOutputView): Unit = ()

  override def deserialize(dataInputView: DataInputView): Product = None

  override def deserialize(t: Product, dataInputView: DataInputView): Product = None

  override def copy(dataInputView: DataInputView, dataOutputView: DataOutputView): Unit = ()

  override def snapshotConfiguration(): TypeSerializerSnapshot[Product] = new NoneTypeSerializer.Snapshot

  override def equals(obj: Any): Boolean = obj.isInstanceOf[NoneTypeSerializer]

  override def hashCode(): Int = 0
}
