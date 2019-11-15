package com.amazon.milan.flink.components

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.amazon.milan.flink.types.ArrayRecord
import com.amazon.milan.test.{IntRecord, KeyValueRecord, StringRecord}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.Assert._
import org.junit.Test


@Test
class TestTupleStreamTypeSerializer {
  @Test
  def test_TupleStreamTypeSerializer_Serialize_ThenDeserialize_WithOneIntField_ReturnsOriginalTuple(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val fields = Array(FieldTypeInformation("i", createTypeInformation[Int]))

    val serializer = new TupleStreamTypeSerializer(env.getConfig, fields)

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

    val originalSerializer = new TupleStreamTypeSerializer(env.getConfig, fields)
    val outputSerializer = ObjectStreamUtil.serializeAndDeserialize(originalSerializer)

    val outputStream = new ByteArrayOutputStream()
    val outputView = new DataOutputViewStreamWrapper(outputStream)
    val originalValue = ArrayRecord.fromElements(1)
    outputSerializer.serialize(originalValue, outputView)

    val bytes = outputStream.toByteArray

    val inputStream = new ByteArrayInputStream(bytes)
    val inputView = new DataInputViewStreamWrapper(inputStream)
    val outputValue = outputSerializer.deserialize(inputView)

    assertTrue(originalValue.sameElements(outputValue))
  }

  @Test
  def test_TupleStreamTypeSerializer_Serialize_ThenDeserialize_WithSeveralObjectFields_ReturnsOriginalTuple(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val fields = Seq(
      FieldTypeInformation("a", createTypeInformation[IntRecord]),
      FieldTypeInformation("b", createTypeInformation[StringRecord]),
      FieldTypeInformation("c", createTypeInformation[KeyValueRecord]))

    val serializer = new TupleStreamTypeSerializer(env.getConfig, fields)

    val outputStream = new ByteArrayOutputStream()
    val outputView = new DataOutputViewStreamWrapper(outputStream)
    val originalValue = ArrayRecord.fromElements(IntRecord(1), StringRecord("b"), KeyValueRecord("foo", "bar"))
    serializer.serialize(originalValue, outputView)

    val bytes = outputStream.toByteArray

    val inputStream = new ByteArrayInputStream(bytes)
    val inputView = new DataInputViewStreamWrapper(inputStream)
    val outputValue = serializer.deserialize(inputView)

    assertTrue(originalValue.sameElements(outputValue))
  }
}
