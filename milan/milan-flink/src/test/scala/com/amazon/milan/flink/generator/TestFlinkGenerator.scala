package com.amazon.milan.flink.generator

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.application.sources.S3DataSource
import com.amazon.milan.dataformats.JsonDataInputFormat
import com.amazon.milan.flink.testing.{IntRecord, TestApplicationExecutor}
import com.amazon.milan.lang._
import com.amazon.milan.testing.applications._
import org.junit.Assert._
import org.junit.Test

import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox


@Test
class TestFlinkGenerator {
  private val generator = new FlinkGenerator(GeneratorConfig())

  @Test
  def test_FlinkGenerator_GenerateScala_WithListSourceAndMapOfOneRecord_GeneratesCodeThatCompilesAndOutputsMappedRecord(): Unit = {
    val input = Stream.of[IntRecord].withName("input")
    val output = input.map(r => IntRecord(r.i + 1)).withName("output")

    val graph = new StreamGraph(output)
    val config = new ApplicationConfiguration
    config.setListSource(input, IntRecord(1))

    val result = TestApplicationExecutor.executeApplication(graph, config, 10, output)
    val outputRecords = result.getRecords(output)
    assertEquals(List(IntRecord(2)), outputRecords)
  }

  @Test
  def test_FlinkGenerator_GenerateScala_WithS3DataSource_GeneratesCodeThatCompiles(): Unit = {
    val input = Stream.of[IntRecord].withName("input")
    val output = input.map(r => IntRecord(r.i + 1)).withName("output")

    val graph = new StreamGraph(output)
    val config = new ApplicationConfiguration
    config.setSource(input, new S3DataSource[IntRecord]("bucket", "prefix", new JsonDataInputFormat[IntRecord]()))

    val generatedCode = this.generator.generateScala(graph, config)

    this.eval(generatedCode)
  }

  private def eval(code: String): Any = {
    try {
      val tb = ToolBox(universe.runtimeMirror(this.getClass.getClassLoader)).mkToolBox()
      val tree = tb.parse(code)
      tb.eval(tree)
    }
    catch {
      case ex: Throwable =>
        Console.println(code)
        throw ex
    }
  }
}
