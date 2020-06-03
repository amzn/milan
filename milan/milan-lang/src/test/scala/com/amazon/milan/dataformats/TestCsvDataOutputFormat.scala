package com.amazon.milan.dataformats

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.junit.Assert._
import org.junit.Test


object TestCsvDataOutputFormat {

  case class Record(a: Int, b: Int, c: Int)

}

import com.amazon.milan.dataformats.TestCsvDataOutputFormat._

@Test
class TestCsvDataOutputFormat {
  @Test
  def test_CsvDataOutputFormat_WriteValue_WithSchemaSpecifiedAndOneFieldNotInSchema_WritesSchemaFields(): Unit = {
    val format = new CsvDataOutputFormat[Record](CsvDataOutputFormat.Configuration.default.withSchema("b", "c"))
    val output = new ByteArrayOutputStream()
    format.writeValue(Record(1, 2, 3), output)

    val outputLine = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(output.toByteArray)).toString.lines.next()
    assertEquals("2,3", outputLine)
  }
}
