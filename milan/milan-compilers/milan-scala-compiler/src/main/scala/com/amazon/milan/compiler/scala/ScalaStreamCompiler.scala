package com.amazon.milan.compiler.scala

import com.amazon.milan.application.ApplicationInstance
import com.amazon.milan.compiler.scala.event.EventHandlerClassGenerator
import com.amazon.milan.tools.{ApplicationInstanceCompiler, CompilerOutputs, InstanceParameters}

import java.io.{FileOutputStream, OutputStream}
import java.nio.file.Path


object ScalaStreamCompiler {
  val DEFAULT_PACKAGENAME = "generated"
  val DEFAULT_CLASSNAME = "MilanApplication"
  val DEFAULT_FUNCTIONNAME = "execute"
}


class ScalaStreamCompiler extends ApplicationInstanceCompiler {
  override def compile(applicationInstance: ApplicationInstance,
                       params: InstanceParameters,
                       outputs: CompilerOutputs): Unit = {
    val output = new FileOutputStream(outputs.getOutput("scala").toFile)

    try {
      this.compile(applicationInstance, params, output)
    }
    finally {
      output.close()
    }
  }

  def compile(applicationInstance: ApplicationInstance,
              params: InstanceParameters,
              output: OutputStream): Unit = {
    val outputStreamId = params.getValue("output")
    val packageName = params.getValueOption("package").getOrElse(ScalaStreamCompiler.DEFAULT_PACKAGENAME)
    val className = params.getValueOption("class").getOrElse(ScalaStreamCompiler.DEFAULT_CLASSNAME)
    val functionName = params.getValueOption("function").getOrElse(ScalaStreamCompiler.DEFAULT_FUNCTIONNAME)

    output.writeUtf8(s"package $packageName\n\n")
    ScalaStreamGenerator.generateFunction(functionName, applicationInstance.application.streams, outputStreamId, output)
    EventHandlerClassGenerator.generateClass(applicationInstance, className, output, plugin = None)
  }
}
