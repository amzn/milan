package com.amazon.milan.control

import java.time.Duration

import com.amazon.milan.flink.control.{ApplicationController, ApplicationControllerDiagnostic, ApplicationControllerMessageHandler, ApplicationControllerProcessFunction, CommandLineFlinkClient}
import com.amazon.milan.manage.{LocalPackageRepository, ProcessCommandExecutor}
import com.amazon.milan.serialization.ObjectSerialization
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.OutputTag
import org.junit.Assert._
import org.junit.Test


@Test
class TestApplicationControllerProcessFunction {
  @Test
  def test_ApplicationControllerProcessFunction_CanSerialize(): Unit = {
    val packageRepository = new LocalPackageRepository("/tmp")
    val flinkClient = new CommandLineFlinkClient(new ProcessCommandExecutor())
    val controller = new ApplicationController(packageRepository, flinkClient, "/tmp", "", "")
    val handler = new ApplicationControllerMessageHandler(controller)
    val diagnosticOutputTag = new OutputTag[ApplicationControllerDiagnostic]("diagnostics")
    val target = new ApplicationControllerProcessFunction(handler, Duration.ofSeconds(1), diagnosticOutputTag, "", Int.MaxValue)

    val bytes = ObjectSerialization.serialize(target)
    assertTrue(bytes.nonEmpty)
  }
}
