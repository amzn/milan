package com.amazon.milan.compiler.flink.types

import com.amazon.milan.compiler.flink.runtime.MilanFlinkRuntimeException
import org.apache.commons.lang.builder.HashCodeBuilder
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot, TypeSerializerSnapshotSerializationUtil}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

import scala.reflect.{ClassTag, classTag}


class ScalaTupleTypeInformation[T >: Null <: Product : ClassTag](val elementTypeInfo: Array[TypeInformation[_]])
  extends TypeInformation[T] {

  override def isBasicType: Boolean = false

  override def isTupleType: Boolean = true

  override def getArity: Int = this.elementTypeInfo.length

  override def getTotalFields: Int = this.getArity + this.elementTypeInfo.map(_.getTotalFields).sum

  override def getTypeClass: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]

  override def isKeyType: Boolean = true

  override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[T] =
    new ScalaTupleTypeSerializer[T](this.elementTypeInfo.map(_.createSerializer(executionConfig)))

  override def canEqual(o: Any): Boolean = o.isInstanceOf[ScalaTupleTypeInformation[T]]

  override def toString: String = s"ScalaTupleTypeInformation(${this.elementTypeInfo.mkString(", ")})"

  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(obj: Any): Boolean = obj match {
    case o: ScalaTupleTypeInformation[_] => this.elementTypeInfo.sameElements(o.elementTypeInfo)
    case _ => false
  }
}


object ScalaTupleTypeSerializer {

  case class ElementSnapshot[T](serializer: TypeSerializer[T], snapshot: TypeSerializerSnapshot[T])

  class Snapshot[T >: Null <: Product : ClassTag](private var elements: Array[ElementSnapshot[_]])
    extends TypeSerializerSnapshot[T] {

    /**
     * Don't use this constructor, it's here to make the class serializable.
     */
    def this() {
      this(Array.empty)
    }

    override def getCurrentVersion: Int = 1

    override def writeSnapshot(output: DataOutputView): Unit = {
      output.writeInt(this.elements.length)
      this.elements.foreach(element =>
        TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
          output,
          element.snapshot.asInstanceOf[TypeSerializerSnapshot[Any]],
          element.serializer.asInstanceOf[TypeSerializer[Any]])
      )
    }

    override def readSnapshot(snapshotVersion: Int,
                              input: DataInputView,
                              classLoader: ClassLoader): Unit = {
      val elementCount = input.readInt()

      val elementSnapshots = Array.tabulate[TypeSerializerSnapshot[_]](elementCount)(_ =>
        TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(input, classLoader, null)
      )

      this.elements = elementSnapshots.map(snapshot => ElementSnapshot(null, snapshot))
    }

    override def restoreSerializer(): TypeSerializer[T] = {
      // Overwrite the elements field to include the serializers for each element.
      this.elements = this.elements.map(element =>
        ElementSnapshot(element.snapshot.restoreSerializer(), element.snapshot)
      )

      new ScalaTupleTypeSerializer[T](this.elements.map(_.serializer))
    }

    override def resolveSchemaCompatibility(typeSerializer: TypeSerializer[T]): TypeSerializerSchemaCompatibility[T] = {
      val elementResults =
        this.elements
          .map(_.asInstanceOf[ElementSnapshot[Any]])
          .map(element => element.snapshot.resolveSchemaCompatibility(element.serializer))

      if (elementResults.exists(_.isIncompatible)) {
        TypeSerializerSchemaCompatibility.incompatible()
      }
      else if (elementResults.exists(_.isCompatibleAfterMigration)) {
        TypeSerializerSchemaCompatibility.compatibleAfterMigration()
      }
      else if (elementResults.forall(_.isCompatibleAsIs)) {
        TypeSerializerSchemaCompatibility.compatibleAsIs()
      }
      else {
        throw new MilanFlinkRuntimeException("Can't resolve schema compatibility because element serializers returned inconsistent or unsupported compatibility check results.")
      }
    }
  }

}

class ScalaTupleTypeSerializer[T >: Null <: Product : ClassTag](val elementSerializers: Array[TypeSerializer[_]])
  extends TypeSerializer[T] {

  private val classTag = implicitly[ClassTag[T]]

  override def isImmutableType: Boolean = true

  override def duplicate(): TypeSerializer[T] = this

  override def createInstance(): T = null

  override def copy(value: T): T = value

  override def copy(source: T, dest: T): T = source

  override def getLength: Int = 0

  override def serialize(value: T, output: DataOutputView): Unit = {
    this.elementSerializers
      .map(_.asInstanceOf[TypeSerializer[Any]])
      .zipWithIndex
      .foreach {
        case (elementSerializer, i) => elementSerializer.serialize(value.productElement(i), output)
      }
  }

  override def deserialize(input: DataInputView): T = {
    if (this.elementSerializers.isEmpty) {
      None.asInstanceOf[T]
    }
    else {
      val elementValues = this.elementSerializers.map(_.deserialize(input)).map(_.asInstanceOf[AnyRef])

      val cls = this.classTag.runtimeClass

      // Assume that if we find a constructor with the correct number of parameters then that's the one we want.
      val constructor = cls.getConstructors.find(_.getParameterCount == this.elementSerializers.length)

      constructor match {
        case Some(cons) =>
          cons.newInstance(elementValues: _*).asInstanceOf[T]

        case None =>
          throw new Exception(s"Can't instantiate tuple class '${cls.getName}' with ${this.elementSerializers.length} elements.")
      }
    }
  }

  override def deserialize(dest: T, input: DataInputView): T =
    this.deserialize(input)

  override def copy(input: DataInputView, output: DataOutputView): Unit =
    this.elementSerializers.foreach(_.copy(input, output))

  override def snapshotConfiguration(): TypeSerializerSnapshot[T] = {
    val elementSnapshots =
      this.elementSerializers
        .map(_.asInstanceOf[TypeSerializer[Any]])
        .map(elementSerializer =>
          ScalaTupleTypeSerializer.ElementSnapshot(elementSerializer, elementSerializer.snapshotConfiguration())
            .asInstanceOf[ScalaTupleTypeSerializer.ElementSnapshot[_]]
        )

    new ScalaTupleTypeSerializer.Snapshot[T](elementSnapshots)
  }

  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(obj: Any): Boolean = obj match {
    case o: ScalaTupleTypeSerializer[T] => this.elementSerializers.sameElements(o.elementSerializers)
    case _ => false
  }
}
