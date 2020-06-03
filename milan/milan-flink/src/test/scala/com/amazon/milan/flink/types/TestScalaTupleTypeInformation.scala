package com.amazon.milan.flink.types

import java.time.Instant

import com.amazon.milan.flink.application.sources.TestS3DataSource.IntRecord
import com.amazon.milan.flink.testing.DateKeyValueRecord
import com.amazon.milan.flink.testutil._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.Assert._
import org.junit.Test


@Test
class TestScalaTupleTypeInformation {
  @Test
  def test_ScalaTupleTypeInformation_WithTupleOfIntAndString_CanSerializerAndDeserializeAndProduceEquivalentObject(): Unit = {
    val typeInfo = new ScalaTupleTypeInformation[(Int, String)](Array(createTypeInformation[Int], createTypeInformation[String]))
    val typeInfoCopy = ObjectStreamUtil.serializeAndDeserialize(typeInfo)
    assertEquals(typeInfo, typeInfoCopy)
  }

  @Test
  def test_ScalaTupleTypeInformation_WithTupleOfComplexTypes_CanSerializerAndDeserializeAndProduceEquivalentObject(): Unit = {
    val typeInfo = new ScalaTupleTypeInformation[(IntRecord, DateKeyValueRecord)](Array(createTypeInformation[IntRecord], createTypeInformation[DateKeyValueRecord]))
    val typeInfoCopy = ObjectStreamUtil.serializeAndDeserialize(typeInfo)
    assertEquals(typeInfo, typeInfoCopy)
  }

  @Test
  def test_ScalaTupleTypeInformation_CreateSerializer_ReturnsSerializerThatCanSerializeAndDeserializeObjects(): Unit = {
    val typeInfo = new ScalaTupleTypeInformation[(Int, String)](Array(createTypeInformation[Int], createTypeInformation[String]))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val serializer = typeInfo.createSerializer(env.getConfig)

    val original = (2, "foo")
    val copy = copyWithSerializer(original, serializer)

    assertEquals(original, copy)
  }

  @Test
  def test_ScalaTupleTypeInformation_CreateSerializer_WithComplexTypes_ReturnsSerializerThatCanSerializeAndDeserializeObjects(): Unit = {
    val typeInfo = new ScalaTupleTypeInformation[(IntRecord, DateKeyValueRecord)](Array(createTypeInformation[IntRecord], createTypeInformation[DateKeyValueRecord]))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val serializer = typeInfo.createSerializer(env.getConfig)

    val original = (IntRecord(2), DateKeyValueRecord(Instant.now(), 5, 10))
    val copy = copyWithSerializer(original, serializer)

    assertEquals(original, copy)
  }
}
