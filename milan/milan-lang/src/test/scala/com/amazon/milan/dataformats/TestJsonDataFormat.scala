package com.amazon.milan.dataformats

import com.amazon.milan.serialization.{DataFormatConfiguration, DataFormatFlags, ScalaObjectMapper}
import com.amazon.milan.typeutil.createTypeDescriptor
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
import org.junit.Assert._
import org.junit.Test


object TestJsonDataFormat {

  case class Record()

  case class GenericRecord[T](i: T, l: List[T])

}

import com.amazon.milan.dataformats.TestJsonDataFormat._


@Test
class TestJsonDataFormat {
  @Test
  def test_JsonDataFormat_SerializeAndDeserializeAsDataFormat_ReturnsEquivalentObject(): Unit = {
    val target = new JsonDataFormat[Record](DataFormatConfiguration.default)
    val json = ScalaObjectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(target)
    val deserialized = ScalaObjectMapper.readValue[DataFormat[Any]](json, classOf[DataFormat[Any]])
    assertEquals(target, deserialized)
  }

  @Test
  def test_JsonDataFormat_SerializeAndDeserializeAsDataFormat_ReturnsSerializerWithEquivalentBehavior(): Unit = {
    val original = new JsonDataFormat[GenericRecord[Integer]](DataFormatConfiguration.default)
    val json = ScalaObjectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(original)
    val copy = ScalaObjectMapper.readValue[DataFormat[GenericRecord[Integer]]](json, classOf[DataFormat[GenericRecord[Integer]]])

    val record = GenericRecord[Integer](5, List(1, 2, 3, 4))
    val recordBytes = ScalaObjectMapper.writeValueAsBytes(record)

    val deserByOriginal = original.readValue(recordBytes, 0, recordBytes.length)
    val deserByCopy = copy.readValue(recordBytes, 0, recordBytes.length)
    assertEquals(deserByOriginal, deserByCopy)
  }

  @Test(expected = classOf[UnrecognizedPropertyException])
  def test_JsonDataFormat_ReadValue_WithFailOnUnknownPropertiesTrueAndExtraPropertyInJson_ThrowsUnrecognizedPropertyException(): Unit = {
    val format = new JsonDataFormat[Record](DataFormatConfiguration.withFlags(DataFormatFlags.FailOnUnknownProperties))
    val jsonBytes = "{ \"unknownProperty\": 0, \"value\": 1 }".getBytes("utf-8")
    format.readValue(jsonBytes, 0, jsonBytes.length)
  }
}
