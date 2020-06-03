package com.amazon.milan.flink.application.sinks

import java.nio.file.Files

import com.amazon.milan.dataformats.JsonDataOutputFormat
import com.amazon.milan.typeutil._
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._


@Test
class TestFileSinkFunction {
  @Test
  def test_FileSinkFunction_WithJsonDataFormat_WithTupleRecordType_WritesOneRecordPerLine(): Unit = {
    val tempFile = Files.createTempFile("test_FileSinkFunction_", ".txt")
    try {
      val dataFormat = new JsonDataOutputFormat[(Int, String)]
      val sinkFunction = new FileSinkFunction[(Int, String)](tempFile.toString, dataFormat)

      sinkFunction.invoke((1, "1"), null)
      sinkFunction.invoke((2, "2"), null)
      sinkFunction.invoke((3, "3"), null)

      val contents = Files.readAllLines(tempFile).asScala.toList
      assertEquals(3, contents.length)
    }
    finally {
      Files.deleteIfExists(tempFile)
    }
  }
}
