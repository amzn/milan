package com.amazon.milan.tools

import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang._
import com.amazon.milan.testing.applications._
import com.amazon.milan.{Id, SemanticVersion}
import org.junit.Assert._
import org.junit.Test

import java.io.{FileOutputStream, OutputStreamWriter}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}


object TestCompileApplicationInstance {

  case class Record(recordId: String, i: Int)

  class Provider extends ApplicationInstanceProvider {
    override def getApplicationInstance(params: InstanceParameters): ApplicationInstance = {
      val input = Stream.of[Record]
      val streams = StreamCollection.build(input)
      val config = new ApplicationConfiguration
      config.setListSource(input, Record("1", 1))

      val instanceId = params.getValue("instanceId")
      val appId = params.getValue("appId")

      new ApplicationInstance(
        instanceId,
        new Application(appId, streams, SemanticVersion.ZERO),
        config)
    }
  }

  class Compiler extends ApplicationInstanceCompiler {
    override def compile(applicationInstance: ApplicationInstance,
                         params: InstanceParameters,
                         outputs: CompilerOutputs): Unit = {
      val output = new FileOutputStream(outputs.getOutput("test").toFile)
      val writer = new OutputStreamWriter(output)
      val testParam = params.getValue("test")
      writer.write(testParam)
      writer.write(applicationInstance.toJsonString)
      writer.close()
      output.close()
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
        s"-Otest=${tempFile.toString}",
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
