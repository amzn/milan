package com.amazon.milan.compiler.flink.types

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot, TypeSerializerSnapshotSerializationUtil}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

import scala.collection.mutable


class PriorityQueueTypeInformation[T](val valueTypeInfo: TypeInformation[T],
                                      ordering: Ordering[T])
  extends TypeInformation[mutable.PriorityQueue[T]] {

  override def isBasicType: Boolean = false

  override def isTupleType: Boolean = false

  override def getArity: Int = 0

  override def getTotalFields: Int = 0

  override def getTypeClass: Class[mutable.PriorityQueue[T]] = classOf[mutable.PriorityQueue[T]]

  override def isKeyType: Boolean = false

  override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[mutable.PriorityQueue[T]] = {
    new PriorityQueueTypeSerializer[T](this.valueTypeInfo.createSerializer(executionConfig), this.ordering)
  }

  override def canEqual(o: Any): Boolean = o.isInstanceOf[PriorityQueueTypeInformation[T]]

  override def toString: String = s"PriorityQueueTypeInformation[${this.valueTypeInfo.toString}]"

  override def equals(obj: Any): Boolean = obj match {
    case o: PriorityQueueTypeInformation[T] => this.valueTypeInfo.equals(o.valueTypeInfo)
    case _ => false
  }

  override def hashCode(): Int = this.valueTypeInfo.hashCode()
}


object PriorityQueueTypeSerializer {

  class Snapshot[T](private var valueSerializer: TypeSerializer[T],
                    private var valueSnapshot: TypeSerializerSnapshot[T],
                    private var ordering: Ordering[T])
    extends TypeSerializerSnapshot[mutable.PriorityQueue[T]] {

    def this() {
      this(null, null, null)
    }

    override def getCurrentVersion: Int = 0

    override def writeSnapshot(output: DataOutputView): Unit = {
      output.writeUTF(ordering.getClass.getName)
      TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(output, this.valueSnapshot, this.valueSerializer)
    }

    override def readSnapshot(snapshotVersion: Int, input: DataInputView, classLoader: ClassLoader): Unit = {
      val orderingClassName = input.readUTF()
      this.ordering = classLoader.loadClass(orderingClassName).newInstance().asInstanceOf[Ordering[T]]
      this.valueSnapshot = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(input, classLoader, null)
    }

    override def restoreSerializer(): TypeSerializer[mutable.PriorityQueue[T]] = {
      this.valueSerializer = this.valueSnapshot.restoreSerializer()
      new PriorityQueueTypeSerializer[T](this.valueSerializer, this.ordering)
    }

    override def resolveSchemaCompatibility(serializer: TypeSerializer[mutable.PriorityQueue[T]]): TypeSerializerSchemaCompatibility[mutable.PriorityQueue[T]] = {
      val valueResult = this.valueSnapshot.resolveSchemaCompatibility(this.valueSerializer)

      if (valueResult.isCompatibleAfterMigration) {
        TypeSerializerSchemaCompatibility.compatibleAfterMigration()
      }
      else if (valueResult.isCompatibleAsIs) {
        TypeSerializerSchemaCompatibility.compatibleAsIs()
      }
      else if (valueResult.isCompatibleWithReconfiguredSerializer) {
        TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
          new PriorityQueueTypeSerializer[T](valueResult.getReconfiguredSerializer, this.ordering))
      }
      else {
        TypeSerializerSchemaCompatibility.incompatible()
      }
    }
  }

}

class PriorityQueueTypeSerializer[T](val valueSerializer: TypeSerializer[T],
                                     ordering: Ordering[T])
  extends TypeSerializer[mutable.PriorityQueue[T]] {

  override def isImmutableType: Boolean = false

  override def duplicate(): TypeSerializer[mutable.PriorityQueue[T]] = this

  override def createInstance(): mutable.PriorityQueue[T] = mutable.PriorityQueue.empty[T](this.ordering)

  override def copy(value: mutable.PriorityQueue[T]): mutable.PriorityQueue[T] = value.clone()

  override def copy(value: mutable.PriorityQueue[T],
                    dest: mutable.PriorityQueue[T]): mutable.PriorityQueue[T] = {
    dest.clear()
    dest.enqueue(value.toList: _*)
    dest
  }

  override def getLength: Int = 0

  override def serialize(value: mutable.PriorityQueue[T], output: DataOutputView): Unit = {
    output.writeInt(value.length)
    value.toList.foreach(item => this.valueSerializer.serialize(item, output))
  }

  override def deserialize(input: DataInputView): mutable.PriorityQueue[T] = {
    val count = input.readInt()
    val builder = mutable.PriorityQueue.newBuilder(this.ordering)
    builder.sizeHint(count)
    builder ++= Seq.tabulate(count)(_ => this.valueSerializer.deserialize(input))
    builder.result()
  }

  override def deserialize(dest: mutable.PriorityQueue[T], input: DataInputView): mutable.PriorityQueue[T] = {
    dest.clear()

    val count = input.readInt()
    val items = List.tabulate(count)(_ => this.valueSerializer.deserialize(input))
    dest.enqueue(items: _*)
    dest
  }

  override def copy(input: DataInputView, output: DataOutputView): Unit = {
    val count = input.readInt()
    output.writeInt(count)

    for (_ <- 1 to 10) {
      this.valueSerializer.copy(input, output)
    }
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[mutable.PriorityQueue[T]] = {
    new PriorityQueueTypeSerializer.Snapshot[T](
      this.valueSerializer,
      this.valueSerializer.snapshotConfiguration(),
      this.ordering)
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: PriorityQueueTypeSerializer[T] => this.valueSerializer.equals(o.valueSerializer)
    case _ => false
  }

  override def hashCode(): Int = this.valueSerializer.hashCode()
}
