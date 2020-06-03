package com.amazon.milan.flink.types

import com.amazon.milan.Id
import com.amazon.milan.flink.types
import com.amazon.milan.typeutil.{FieldDescriptor, ObjectTypeDescriptor, TypeDescriptor}
import org.apache.commons.lang.builder.HashCodeBuilder
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}


object AggregatorOutputRecord {
  def wrap[T >: Null](value: T): AggregatorOutputRecord[T] =
    new AggregatorOutputRecord[T](value)

  def createTypeDescriptor(valueType: TypeDescriptor[_]): TypeDescriptor[_] = {
    val genericType = TypeDescriptor.of[AggregatorOutputRecord[Any]]

    val fields = genericType.fields
      .map(field =>
        if (field.name == "value") {
          FieldDescriptor(field.name, valueType)
        }
        else {
          field
        }
      )

    new ObjectTypeDescriptor[Any](
      genericType.typeName,
      List(valueType),
      fields)
  }
}


class AggregatorOutputRecord[T >: Null](var outputId: String, var value: T) extends Serializable {
  def this(value: T) {
    this(Id.newId(), value)
  }

  def this() {
    this("", null)
  }
}


object AggregatorOutputRecordTypeInformation {
  def wrap[T >: Null](valueTypeInfo: TypeInformation[T]): AggregatorOutputRecordTypeInformation[T] =
    new AggregatorOutputRecordTypeInformation[T](valueTypeInfo)
}


class AggregatorOutputRecordTypeInformation[T >: Null](val valueTypeInfo: TypeInformation[T])
  extends TypeInformation[AggregatorOutputRecord[T]] {

  override def isBasicType: Boolean = false

  override def isTupleType: Boolean = false

  override def getArity: Int = 1

  override def getTotalFields: Int = 1 + this.valueTypeInfo.getTotalFields

  override def getTypeClass: Class[AggregatorOutputRecord[T]] = classOf[AggregatorOutputRecord[T]]

  override def isKeyType: Boolean = false

  override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[AggregatorOutputRecord[T]] = {
    val baseSerializer = valueTypeInfo.createSerializer(executionConfig)
    new AggregatorOutputRecordSerializer[T](baseSerializer)
  }

  override def canEqual(o: Any): Boolean = o.isInstanceOf[AggregatorOutputRecordTypeInformation[T]]

  override def toString: String = s"AggregatorOutputRecord[${this.valueTypeInfo}]"

  override def equals(obj: Any): Boolean = obj match {
    case o: AggregatorOutputRecordTypeInformation[T] => this.valueTypeInfo.equals(o.valueTypeInfo)
    case _ => false
  }

  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)
}


object AggregatorOutputRecordSerializer {

  class ConfigSnapshot[T >: Null](valueSnapshot: TypeSerializerSnapshot[T])
    extends TypeSerializerSnapshot[AggregatorOutputRecord[T]]
      with Serializable {

    /**
     * Don't use this constructor, it's here to make the class serializable.
     */
    def this() {
      this(null)
    }

    override def getCurrentVersion: Int = this.valueSnapshot.getCurrentVersion

    override def writeSnapshot(dataOutputView: DataOutputView): Unit = {
      this.valueSnapshot.writeSnapshot(dataOutputView)
    }

    override def readSnapshot(snapshotVersion: Int, dataInputView: DataInputView, classLoader: ClassLoader): Unit = {
      this.valueSnapshot.readSnapshot(snapshotVersion, dataInputView, classLoader)
    }

    override def restoreSerializer(): TypeSerializer[AggregatorOutputRecord[T]] = {
      new AggregatorOutputRecordSerializer[T](this.valueSnapshot.restoreSerializer())
    }

    override def resolveSchemaCompatibility(typeSerializer: TypeSerializer[AggregatorOutputRecord[T]]): TypeSerializerSchemaCompatibility[AggregatorOutputRecord[T]] = {
      val valueResult = this.valueSnapshot.resolveSchemaCompatibility(typeSerializer.asInstanceOf[AggregatorOutputRecordSerializer[T]].valueSerializer)

      if (valueResult.isCompatibleAsIs) {
        TypeSerializerSchemaCompatibility.compatibleAsIs()
      }
      else if (valueResult.isCompatibleAfterMigration) {
        TypeSerializerSchemaCompatibility.compatibleAfterMigration()
      }
      else if (valueResult.isCompatibleWithReconfiguredSerializer) {
        TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
          new AggregatorOutputRecordSerializer[T](valueResult.getReconfiguredSerializer))
      }
      else {
        TypeSerializerSchemaCompatibility.incompatible()
      }
    }
  }

}

class AggregatorOutputRecordSerializer[T >: Null](val valueSerializer: TypeSerializer[T]) extends TypeSerializer[AggregatorOutputRecord[T]] {
  override def isImmutableType: Boolean = true

  override def duplicate(): TypeSerializer[AggregatorOutputRecord[T]] = this

  override def createInstance(): AggregatorOutputRecord[T] =
    new AggregatorOutputRecord[T](this.valueSerializer.createInstance())

  override def copy(value: AggregatorOutputRecord[T]): AggregatorOutputRecord[T] = {
    new AggregatorOutputRecord[T](value.outputId, this.valueSerializer.copy(value.value))
  }

  override def copy(value: AggregatorOutputRecord[T], dest: AggregatorOutputRecord[T]): AggregatorOutputRecord[T] = {
    this.copy(value)
  }

  override def getLength: Int = -1

  override def serialize(value: AggregatorOutputRecord[T], output: DataOutputView): Unit = {
    output.writeUTF(value.outputId)
    this.valueSerializer.serialize(value.value, output)
  }

  override def deserialize(input: DataInputView): AggregatorOutputRecord[T] = {
    val outputId = input.readUTF()
    val value = this.valueSerializer.deserialize(input)
    new AggregatorOutputRecord[T](outputId, value)
  }

  override def deserialize(dest: AggregatorOutputRecord[T],
                           input: DataInputView): AggregatorOutputRecord[T] = {
    this.deserialize(input)
  }

  override def copy(input: DataInputView, output: DataOutputView): Unit = {
    output.writeUTF(input.readUTF())
    this.valueSerializer.copy(input, output)
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[AggregatorOutputRecord[T]] = {
    new types.AggregatorOutputRecordSerializer.ConfigSnapshot[T](this.valueSerializer.snapshotConfiguration())
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: AggregatorOutputRecordSerializer[T] => this.valueSerializer.equals(o.valueSerializer)
    case _ => false
  }

  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)
}
