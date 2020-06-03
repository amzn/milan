package com.amazon.milan.flink.application.sinks

import java.nio.file.Files

import com.amazon.milan.application.sinks.FileDataSink
import com.amazon.milan.dataformats.CsvDataOutputFormat
import com.amazon.milan.flink.testing._
import com.amazon.milan.serialization.ScalaObjectMapper
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._


@Test
class TestFlinkFileDataSink {
  @Test
  def test_FlinkFileDataSink_InFlinkApp_DeserializedFromFileDataSink_WithCsvDataFormat_WritesCsvRecords(): Unit = {
    val tempFile = Files.createTempFile("testFlinkFileDataSink", ".csv")

    try {
      val sink = new FileDataSink[IntKeyValueRecord](tempFile.toString, new CsvDataOutputFormat[IntKeyValueRecord]())
      val json = ScalaObjectMapper.writeValueAsString(sink)
      val flinkSink = ScalaObjectMapper.readValue[FlinkFileDataSink[IntKeyValueRecord]](json, classOf[FlinkFileDataSink[IntKeyValueRecord]])
      val sinkFunction = flinkSink.getSinkFunction

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val items = List(IntKeyValueRecord(1, 10), IntKeyValueRecord(2, 20))
      val stream = env.fromCollection(items.asJavaCollection)
      stream.addSink(sinkFunction).setParallelism(flinkSink.getMaxParallelism.get)

      env.execute()

      val fileContents = Files.readAllLines(tempFile).asScala.toList
      assertEquals(2, fileContents.length)
      assertTrue(fileContents.exists(_.contains(",1,10")))
      assertTrue(fileContents.exists(_.contains(",2,20")))
    }
    finally {
      Files.deleteIfExists(tempFile)
    }
  }
}
