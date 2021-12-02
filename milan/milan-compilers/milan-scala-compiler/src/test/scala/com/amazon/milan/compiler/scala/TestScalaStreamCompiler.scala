package com.amazon.milan.compiler.scala

import com.amazon.milan.SemanticVersion
import com.amazon.milan.application.sinks.ConsoleDataSink
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.compiler.scala.testing.IntRecord
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang._
import org.junit.Assert._
import org.junit.Test

import java.io.ByteArrayOutputStream


@Test
class TestScalaStreamCompiler {
  @Test
  def test_ScalaStreamCompiler_Compile_OutputsCode(): Unit = {
    val input = Stream.of[IntRecord].withId("input")
    val output = input.map(r => IntRecord(r.i + 1)).withId("output")

    val streams = StreamCollection.build(output)
    val app = new Application("app", streams, SemanticVersion.ZERO)
    val config = new ApplicationConfiguration()
    config.addSink(output, new ConsoleDataSink[IntRecord])
    val instance = new ApplicationInstance("instance", app, config)

    val params = List(("package", "testPackage"), ("class", "TestClass"), ("output", "output"), ("function", "testMethod"))

    val compiler = new ScalaStreamCompiler

    val outputStream = new ByteArrayOutputStream()
    compiler.compile(instance, params, outputStream)

    val classCode = outputStream.toString

    assertTrue(classCode.startsWith("package testPackage"))
    assertTrue(classCode.contains("TestClass"))
    assertTrue(classCode.contains("output: "))
    assertTrue(classCode.contains("def testMethod"))
  }
}
