package com.amazon.milan.flink.application.sources

import java.nio.file.Files
import java.time.Duration

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.application.sinks.SingletonMemorySink
import com.amazon.milan.application.sources.FileDataSource
import com.amazon.milan.dataformats.{CsvDataFormat, JsonDataFormat}
import com.amazon.milan.flink.testing._
import com.amazon.milan.lang.{Stream, StreamGraph}
import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.test.IntRecord
import com.amazon.milan.testing.Concurrent
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._


@Test
class TestFileDataSource {
  @Test
  def test_FileDataSource_WithCsvFile_ReturnsDataStreamThatYieldsExpectedIntRecordObjectsWhenExecuted(): Unit = {
    val tempSourceFile = Files.createTempFile("milan-test-", ".csv")

    try {
      val contents = Seq("1", "2", "3").asJava
      Files.write(tempSourceFile, contents)

      val dataFormat = new CsvDataFormat[IntRecord](Array("i"))
      val source = new FileDataSource[IntRecord](tempSourceFile.toString, dataFormat)

      val stream = Stream.of[IntRecord]

      val graph = new StreamGraph(stream)

      val sink = new SingletonMemorySink[IntRecord]()
      val config = new ApplicationConfiguration()
      config.setSource(stream, source)
      config.addSink(stream, sink)

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      compileFromSerialized(graph, config, env)

      assertTrue(
        Concurrent.executeAndWait(
          () => env.execute(),
          () => sink.getRecordCount == 3,
          Duration.ofSeconds(10)))

      val outputs = sink.getValues.sortBy(_.i).toArray
      assertEquals(1, outputs(0).i)
      assertEquals(2, outputs(1).i)
      assertEquals(3, outputs(2).i)
    }
    finally {
      Files.deleteIfExists(tempSourceFile)
    }
  }

  @Test
  def test_FileDataSource_AddSource_WithJsonFile_ReturnsDataStreamThatYieldsExpectedIntRecordObjectsWhenExecuted(): Unit = {
    val tempSourceFile = Files.createTempFile("milan-test-", ".csv")

    try {
      val records = Array(IntRecord(1), IntRecord(2), IntRecord(3))

      val contents = records.map(ScalaObjectMapper.writeValueAsString).toSeq.asJava
      Files.write(tempSourceFile, contents)

      val dataFormat = new JsonDataFormat[IntRecord]()
      val source = new FileDataSource[IntRecord](tempSourceFile.toString, dataFormat)

      val stream = Stream.of[IntRecord]
      val graph = new StreamGraph(stream)

      val sink = new SingletonMemorySink[IntRecord]()
      val config = new ApplicationConfiguration()
      config.setSource(stream, source)
      config.addSink(stream, sink)

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      compileFromSerialized(graph, config, env)

      assertTrue(
        Concurrent.executeAndWait(
          () => env.execute(),
          () => sink.getRecordCount == 3,
          Duration.ofSeconds(10)))

      val outputs = sink.getValues.sortBy(_.i).toArray
      assertEquals(1, outputs(0).i)
      assertEquals(2, outputs(1).i)
      assertEquals(3, outputs(2).i)
    }
    finally {
      Files.deleteIfExists(tempSourceFile)
    }
  }
}
