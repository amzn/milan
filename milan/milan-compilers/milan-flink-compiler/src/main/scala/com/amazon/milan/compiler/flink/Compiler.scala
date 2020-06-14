package com.amazon.milan.compiler.flink

import java.io.OutputStream

import com.amazon.milan.application.ApplicationInstance
import com.amazon.milan.compiler.flink.generator.FlinkGenerator
import com.amazon.milan.tools.ApplicationInstanceCompiler


object Compiler {
  val DEFAULT_PACKAGENAME = "generated"

  val DEFAULT_CLASSNAME = "MilanApplication"
}

/**
 * Provides a [[ApplicationInstanceCompiler]] interface to the Flink generator.
 */
class Compiler extends ApplicationInstanceCompiler {
  override def compile(applicationInstance: ApplicationInstance,
                       params: List[(String, String)],
                       output: OutputStream): Unit = {

    val packageName = params.find(_._1 == "package").map(_._2).getOrElse(Compiler.DEFAULT_PACKAGENAME)
    val className = params.find(_._1 == "class").map(_._2).getOrElse(Compiler.DEFAULT_CLASSNAME)

    FlinkGenerator.default.generateScala(applicationInstance, output, packageName, className)
  }
}
