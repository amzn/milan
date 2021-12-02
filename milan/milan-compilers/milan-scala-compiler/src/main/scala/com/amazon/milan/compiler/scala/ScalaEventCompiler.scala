package com.amazon.milan.compiler.scala

import com.amazon.milan.application.ApplicationInstance
import com.amazon.milan.compiler.scala.event.EventHandlerClassGenerator
import com.amazon.milan.tools.ApplicationInstanceCompiler

import java.io.OutputStream


object ScalaEventCompiler {
  val DEFAULT_PACKAGENAME = "generated"

  val DEFAULT_CLASSNAME = "MilanApplication"
}


class ScalaEventCompiler extends ApplicationInstanceCompiler {
  override def compile(applicationInstance: ApplicationInstance,
                       params: List[(String, String)],
                       output: OutputStream): Unit = {
    val packageName = params.find(_._1 == "package").map(_._2).getOrElse(ScalaEventCompiler.DEFAULT_PACKAGENAME)
    val className = params.find(_._1 == "class").map(_._2).getOrElse(ScalaEventCompiler.DEFAULT_CLASSNAME)

    output.writeUtf8(s"package $packageName\n\n")

    EventHandlerClassGenerator.generateClass(applicationInstance, className, output)
  }
}
