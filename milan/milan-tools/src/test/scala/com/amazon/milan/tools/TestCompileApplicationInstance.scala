package com.amazon.milan.tools

import java.io.{OutputStream, OutputStreamWriter}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang._
import com.amazon.milan.testing.applications._
import com.amazon.milan.{Id, SemanticVersion}
import org.junit.Assert._
import org.junit.Test


object TestCompileApplicationInstance {

  case class Record(recordId: String, i: Int)

  class Provider extends ApplicationInstanceProvider {
    override def getApplicationInstance(params: List[(String, String)]): ApplicationInstance = {
      val input = Stream.of[Record]
      val streams = StreamCollection.build(input)
      val config = new ApplicationConfiguration
      config.setListSource(input, Record("1", 1))

      val instanceId = params.find(_._1 == "instanceId").get._2
      val appId = params.find(_._1 == "appId").get._2

      new ApplicationInstance(
        instanceId,
        new Application(appId, streams, SemanticVersion.ZERO),
        config)
    }
  }

  class Compiler extends ApplicationInstanceCompiler {
    override def compile(applicationInstance: ApplicationInstance,
                         params: List[(String, String)],
                         output: OutputStream): Unit = {
      val writer = new OutputStreamWriter(output)
      val testParam = params.find(_._1 == "test").get._2
      writer.write(testParam)
      writer.write(applicationInstance.toJsonString)
      writer.close()
    }
  }

}

@Test
class TestCompileApplicationInstance {
  @Test
  def test_CompileApplicationInstance_Main_SendsProviderAndCompilerParameters(): Unit = {

    val tempFile = Files.createTempFile("TestCompileApplicationInstance", ".scala")
    Files.deleteIfExists(tempFile)

    val appId = Id.newId()
    val instanceId = Id.newId()
    val testValue = Id.newId()

    try {
      val args = Array(
        "--provider",
        "com.amazon.milan.tools.TestCompileApplicationInstance.Provider",
        "--compiler",
        "com.amazon.milan.tools.TestCompileApplicationInstance.Compiler",
        "--package",
        "generated",
        "--output",
        tempFile.toString,
        s"-PinstanceId=$instanceId",
        s"-PappId=$appId",
        s"-Ctest=$testValue"
      )
      CompileApplicationInstance.main(args)

      val fileContents = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(tempFile))).toString
      assertTrue(fileContents.contains(appId))
      assertTrue(fileContents.contains(instanceId))
      assertTrue(fileContents.contains(testValue))
    }
    finally {
      Files.deleteIfExists(tempFile)
    }
  }
}
