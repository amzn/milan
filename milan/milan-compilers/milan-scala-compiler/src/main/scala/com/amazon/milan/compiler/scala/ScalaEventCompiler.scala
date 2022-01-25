package com.amazon.milan.compiler.scala

import com.amazon.milan.application.ApplicationInstance
import com.amazon.milan.compiler.scala.event.EventHandlerClassGenerator
import com.amazon.milan.tools.{ApplicationInstanceCompiler, InstanceParameters, CompilerOutputs}

import java.io.{FileOutputStream, OutputStream}
import java.nio.file.Path


object ScalaEventCompiler {
  val DEFAULT_PACKAGENAME = "generated"

  val DEFAULT_CLASSNAME = "MilanApplication"
}


class ScalaEventCompiler extends ApplicationInstanceCompiler {
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
    val packageName = params.getValueOption("package").getOrElse(ScalaEventCompiler.DEFAULT_PACKAGENAME)
    val className = params.getValueOption("class").getOrElse(ScalaEventCompiler.DEFAULT_CLASSNAME)

    output.writeUtf8(s"package $packageName\n\n")
    EventHandlerClassGenerator.generateClass(applicationInstance, className, output)
  }
}
