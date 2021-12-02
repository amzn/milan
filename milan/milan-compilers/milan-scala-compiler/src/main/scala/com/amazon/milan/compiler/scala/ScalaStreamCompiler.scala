package com.amazon.milan.compiler.scala

import com.amazon.milan.application.ApplicationInstance
import com.amazon.milan.compiler.scala.event.EventHandlerClassGenerator
import com.amazon.milan.tools.ApplicationInstanceCompiler

import java.io.OutputStream


object ScalaStreamCompiler {
  val DEFAULT_PACKAGENAME = "generated"
  val DEFAULT_CLASSNAME = "MilanApplication"
  val DEFAULT_FUNCTIONNAME = "execute"
}


class ScalaStreamCompiler extends ApplicationInstanceCompiler {
  override def compile(applicationInstance: ApplicationInstance,
                       params: List[(String, String)],
                       output: OutputStream): Unit = {
    val outputStreamId = params.find(_._1 == "output").map(_._2).get
    val packageName = params.find(_._1 == "package").map(_._2).getOrElse(ScalaStreamCompiler.DEFAULT_PACKAGENAME)
    val className = params.find(_._1 == "class").map(_._2).getOrElse(ScalaStreamCompiler.DEFAULT_CLASSNAME)
    val functionName = params.find(_._1 == "function").map(_._2).getOrElse(ScalaStreamCompiler.DEFAULT_FUNCTIONNAME)

    output.writeUtf8(s"package $packageName\n\n")

    ScalaStreamGenerator.generateFunction(functionName, applicationInstance.application.streams, outputStreamId, output)
    EventHandlerClassGenerator.generateClass(applicationInstance, className, output)
  }
}
