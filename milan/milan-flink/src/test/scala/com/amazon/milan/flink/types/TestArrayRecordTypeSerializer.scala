package com.amazon.milan.flink.types

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.amazon.milan.flink.testing.{IntRecord, KeyValueRecord, StringRecord}
import com.amazon.milan.flink.testutil.ObjectStreamUtil
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshotSerializationUtil}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.Assert._
import org.junit.Test


@Test
class TestArrayRecordTypeSerializer {
  @Test
  def test_TupleStreamTypeSerializer_Serialize_ThenDeserialize_WithOneIntField_ReturnsOriginalTuple(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val fields = Array(FieldTypeInformation("i", createTypeInformation[Int]))

    val serializer = new ArrayRecordTypeSerializer(env.getConfig, fields)

    val outputStream = new ByteArrayOutputStream()
    val outputView = new DataOutputViewStreamWrapper(outputStream)
    val originalValue = ArrayRecord.fromElements(1)
    serializer.serialize(originalValue, outputView)

    val bytes = outputStream.toByteArray

    val inputStream = new ByteArrayInputStream(bytes)
    val inputView = new DataInputViewStreamWrapper(inputStream)
    val outputValue = serializer.deserialize(inputView)

    assertTrue(originalValue.sameElements(outputValue))
  }

  @Test
  def test_TupleStreamTypeSerializer_Serialize_ThenDeserialize_AfterSerializingAndDeserializingTheSerializerViaObjectStream_ReturnsOriginalTuple(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val fields = Array(FieldTypeInformation("i", createTypeInformation[Int]))

    val originalSerializer = new ArrayRecordTypeSerializer(env.getConfig, fields)
    val outputSerializer = ObjectStreamUtil.serializeAndDeserialize(originalSerializer)

    val originalValue = ArrayRecord.fromElements(1)
    val outputValue = this.copy(originalValue, outputSerializer)

    assertTrue(originalValue.sameElements(outputValue))
  }

  @Test
  def test_TupleStreamTypeSerializer_Serialize_ThenDeserialize_WithSeveralObjectFields_ReturnsOriginalTuple(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val fields = Seq(
      FieldTypeInformation("a", createTypeInformation[IntRecord]),
      FieldTypeInformation("b", createTypeInformation[StringRecord]),
      FieldTypeInformation("c", createTypeInformation[KeyValueRecord]))

    val serializer = new ArrayRecordTypeSerializer(env.getConfig, fields)

    val originalValue = ArrayRecord.fromElements(IntRecord(1), StringRecord("b"), KeyValueRecord("foo", "bar"))
    val outputValue = this.copy(originalValue, serializer)

    assertTrue(originalValue.sameElements(outputValue))
  }

  @Test
  def test_TupleStreamTypeSerializer_Snapshot_WriteSnapshot_ThenReadSnapshot_ThenRestoreSerializer_CreatesFunctionalSerializer(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val fields = Seq(
      FieldTypeInformation("a", createTypeInformation[IntRecord]),
      FieldTypeInformation("b", createTypeInformation[StringRecord]),
      FieldTypeInformation("c", createTypeInformation[KeyValueRecord]))

    val serializer = new ArrayRecordTypeSerializer(env.getConfig, fields)

    val originalSnapshot = serializer.snapshotConfiguration()

    val outputBuffer = new ByteArrayOutputStream()
    val outputView = new DataOutputViewStreamWrapper(outputBuffer)

    TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(outputView, originalSnapshot, serializer)

    val inputBuffer = new ByteArrayInputStream(outputBuffer.toByteArray)
    val inputView = new DataInputViewStreamWrapper(inputBuffer)

    val snapshotCopy = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(inputView, getClass.getClassLoader, null)
    val serializerCopy = snapshotCopy.restoreSerializer().asInstanceOf[TypeSerializer[ArrayRecord]]

    val originalValue = ArrayRecord.fromElements(IntRecord(1), StringRecord("b"), KeyValueRecord("foo", "bar"))
    val outputValue = this.copy(originalValue, serializerCopy)

    assertTrue(originalValue.sameElements(outputValue))
  }

  private def copy[T](value: T, serializer: TypeSerializer[T]): T = {
    val outputStream = new ByteArrayOutputStream()
    val outputView = new DataOutputViewStreamWrapper(outputStream)
    serializer.serialize(value, outputView)

    val bytes = outputStream.toByteArray
    val inputStream = new ByteArrayInputStream(bytes)
    val inputView = new DataInputViewStreamWrapper(inputStream)

    serializer.deserialize(inputView)
  }
}
