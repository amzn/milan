package com.amazon.milan.dataformats

import com.amazon.milan.serialization.{DataFormatConfiguration, DataFormatFlags, ScalaObjectMapper}
import com.amazon.milan.typeutil.createTypeDescriptor
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
import org.junit.Assert._
import org.junit.Test


object TestJsonDataInputFormat {

  case class Record()

  case class GenericRecord[T](i: T, l: List[T])

}

import com.amazon.milan.dataformats.TestJsonDataInputFormat._


@Test
class TestJsonDataInputFormat {
  @Test
  def test_JsonDataInputFormat_SerializeAndDeserializeAsDataFormat_ReturnsEquivalentObject(): Unit = {
    val target = new JsonDataInputFormat[Record](DataFormatConfiguration.default)
    val json = ScalaObjectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(target)
    val deserialized = ScalaObjectMapper.readValue[DataInputFormat[Any]](json, classOf[DataInputFormat[Any]])
    assertEquals(target, deserialized)
  }

  @Test
  def test_JsonDataInputFormat_SerializeAndDeserializeAsDataFormat_ReturnsSerializerWithEquivalentBehavior(): Unit = {
    val original = new JsonDataInputFormat[GenericRecord[Integer]](DataFormatConfiguration.default)
    val json = ScalaObjectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(original)
    val copy = ScalaObjectMapper.readValue[DataInputFormat[GenericRecord[Integer]]](json, classOf[DataInputFormat[GenericRecord[Integer]]])

    val record = GenericRecord[Integer](5, List(1, 2, 3, 4))
    val recordBytes = ScalaObjectMapper.writeValueAsBytes(record)

    val deserByOriginal = original.readValue(recordBytes, 0, recordBytes.length).get
    val deserByCopy = copy.readValue(recordBytes, 0, recordBytes.length).get
    assertEquals(deserByOriginal, deserByCopy)
  }

  @Test(expected = classOf[UnrecognizedPropertyException])
  def test_JsonDataInputFormat_ReadValue_WithFailOnUnknownPropertiesTrueAndExtraPropertyInJson_ThrowsUnrecognizedPropertyException(): Unit = {
    val format = new JsonDataInputFormat[Record](DataFormatConfiguration.withFlags(DataFormatFlags.FailOnUnknownProperties))
    val jsonBytes = "{ \"unknownProperty\": 0, \"value\": 1 }".getBytes("utf-8")
    format.readValue(jsonBytes, 0, jsonBytes.length).get
  }
}
