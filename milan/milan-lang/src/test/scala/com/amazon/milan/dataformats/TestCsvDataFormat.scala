package com.amazon.milan.dataformats

import com.amazon.milan.serialization.{DataFormatConfiguration, DataFormatFlags, ScalaObjectMapper}
import com.amazon.milan.typeutil._
import org.junit.Assert._
import org.junit.Test


object TestCsvDataFormat {

  class TestClass(var intValue: Int, var stringValue: String, var doubleValue: Double) {
    def this() {
      this(0, "", 0)
    }
  }

}

import com.amazon.milan.dataformats.TestCsvDataFormat._


@Test
class TestCsvDataFormat {
  @Test
  def test_CsvDataFormat_ReadValue_WithUtf8EncodedCsvRow_ReturnsCorrectObject(): Unit = {
    val format = new CsvDataFormat[TestClass](Array("intValue", "stringValue", "doubleValue"), DataFormatConfiguration.default)
    val row = "1,\"foo bar\",3.14"
    val rowBytes = row.getBytes("utf-8")
    val output = format.readValue(rowBytes, 0, rowBytes.length)

    assertEquals(1, output.intValue)
    assertEquals("foo bar", output.stringValue)
    assertEquals(3.14, output.doubleValue, 1e-10)
  }

  @Test
  def test_CsvDataFormat_ReadValue_WithUtf8EncodedCsvRowWithOneFieldMissing_ReturnsObjectWithDefaultValueForThatField(): Unit = {
    val format = new CsvDataFormat[TestClass](Array("intValue", "stringValue", "doubleValue"), DataFormatConfiguration.default)
    val row = "1,\"foo bar\""
    val rowBytes = row.getBytes("utf-8")
    val output = format.readValue(rowBytes, 0, rowBytes.length)

    assertEquals(1, output.intValue)
    assertEquals("foo bar", output.stringValue)
    assertEquals(0.0, output.doubleValue, 0)
  }

  @Test(expected = classOf[PropertyNotFoundException])
  def test_CsvDataFormat_ReadValue_WithFailOnUnknownPropertiesTrue_AndUnknownPropertyInSchema_ThrowsUnrecognizedPropertyException(): Unit = {
    val format = new CsvDataFormat[TestClass](Array("unknownProperty"), DataFormatConfiguration.withFlags(DataFormatFlags.FailOnUnknownProperties))
    val row = "1"
    val rowBytes = row.getBytes("utf-8")
    format.readValue(rowBytes, 0, rowBytes.length)
  }

  @Test
  def test_CsvDataFormat_ReadValue_WithFailOnUnknownPropertiesFalse_AndUnknownPropertyInSchema_DoesNotThrow(): Unit = {
    val format = new CsvDataFormat[TestClass](Array("unknownProperty"), DataFormatConfiguration.default)
    val row = "1"
    val rowBytes = row.getBytes("utf-8")
    format.readValue(rowBytes, 0, rowBytes.length)
  }

  @Test
  def test_CsvDataFormat_WithFailOnUnknownPropertiesTrue_JsonSerializeAndDeserializeAsDataFormat_YieldsEquivalentObject(): Unit = {
    val original = new CsvDataFormat[TestClass](
      Array("intValue", "stringValue", "doubleValue"),
      DataFormatConfiguration.withFlags(DataFormatFlags.FailOnUnknownProperties))

    val copy = ScalaObjectMapper.copy(original.asInstanceOf[DataFormat[TestClass]])

    assertEquals(original, copy)
  }
}
