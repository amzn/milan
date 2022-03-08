package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.SemanticVersion
import com.amazon.milan.application.sinks.DynamoDbTableSink
import com.amazon.milan.application.sources.DynamoDbStreamSource
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.compiler.scala.testing.IntRecord
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang._
import com.amazon.milan.tools.ParameterListInstanceParameters
import com.google.common.jimfs.{Configuration, Jimfs}
import org.junit.Assert._
import org.junit.Test

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._


@Test
class TestAwsServerlessCompiler {
  @Test
  def test_AwsServerlessCompiler_Compile_WithPartitionedApplication_ConnectsPartitionsWitSqs(): Unit = {
    // Define an application that has two stateful computations.
    val input = Stream.of[IntRecord].withId("input")
    val maxBy = input.maxBy(r => r.i).withId("max")
    val output = maxBy.sumBy(r => r.i, (_, s) => IntRecord(s)).withId("sum")

    val config = new ApplicationConfiguration()
    config.setSource(input, new DynamoDbStreamSource[IntRecord]())
    config.addSink(output, new DynamoDbTableSink[IntRecord]("OutputTable"))

    val application = new Application("App", StreamCollection.build(output), SemanticVersion.ZERO)
    val instance = new ApplicationInstance(application, config)

    val compiler = new AwsServerlessCompiler
    val fs = Jimfs.newFileSystem(Configuration.unix())
    val scalaRoot = fs.getPath("scala")
    val cdkRoot = fs.getPath("cdk")

    val params = new ParameterListInstanceParameters(
      List("class" -> "Test", "package" -> "test", "partition" -> "true", "code" -> "output.jar")
    )

    compiler.compile(instance, params, scalaRoot, cdkRoot)

    val scalaFiles = Files.list(scalaRoot).iterator().asScala.toList
    assertEquals(2, scalaFiles.size)

    val cdkFiles = Files.list(cdkRoot).iterator().asScala.toList
    assertEquals(3, cdkFiles.size)

    // Check for some expected contents of the "max" partition files.
    val maxScalaFile = this.getFileContents(scalaFiles.find(_.toString.contains("max")).get)
    assertTrue(maxScalaFile.contains("package test"))
    assertTrue(maxScalaFile.contains("class TestApp_max"))

    val maxCdkFilePath = cdkFiles.find(_.toString.contains("max")).get
    val maxCdkFile = this.getFileContents(maxCdkFilePath)
    assertTrue(maxCdkFile.contains("test.TestApp_max::handleRequest"))

    // Check for some expected contents of the "sum" partition files.
    val sumScalaFile = this.getFileContents(scalaFiles.find(_.toString.contains("sum")).get)
    assertTrue(sumScalaFile.contains("package test"))
    assertTrue(sumScalaFile.contains("class TestApp_sum"))

    val sumCdkFilePath = cdkFiles.find(_.toString.contains("sum")).get
    val sumCdkFile = this.getFileContents(sumCdkFilePath)
    assertTrue(sumCdkFile.contains("test.TestApp_sum::handleRequest"))

    // Check for some expected contents of the application file.
    val appCdkFile = this.getFileContents(cdkFiles.find(_.toString.contains("constructApp")).get)
    assertTrue(appCdkFile.contains("import { constructApp_maxLambdaFunction } from \"./TestApp_max\";"))
    assertTrue(appCdkFile.contains("import { constructApp_sumLambdaFunction } from \"./TestApp_sum\";"))
  }

  private def getFileContents(path: Path): String = {
    StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(path))).toString
  }
}
