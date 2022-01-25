package com.amazon.milan.compiler.flink

import com.amazon.milan.application.ApplicationInstance
import com.amazon.milan.compiler.flink.generator.FlinkGenerator
import com.amazon.milan.tools.{ApplicationInstanceCompiler, CompilerOutputs, InstanceParameters}

import java.io.FileOutputStream


object Compiler {
  val DEFAULT_PACKAGENAME = "generated"

  val DEFAULT_CLASSNAME = "MilanApplication"
}

/**
 * Provides a [[ApplicationInstanceCompiler]] interface to the Flink generator.
 */
class Compiler extends ApplicationInstanceCompiler {
  override def compile(applicationInstance: ApplicationInstance,
                       params: InstanceParameters,
                       outputs: CompilerOutputs): Unit = {

    val packageName = params.getValueOption("package").getOrElse(Compiler.DEFAULT_PACKAGENAME)
    val className = params.getValueOption("class").getOrElse(Compiler.DEFAULT_CLASSNAME)

    val outputStream = new FileOutputStream(outputs.getOutput("scala").toFile)
    try {
      FlinkGenerator.default.generateScala(applicationInstance, outputStream, packageName, className)
    }
    finally {
      outputStream.close()
    }
  }
}
