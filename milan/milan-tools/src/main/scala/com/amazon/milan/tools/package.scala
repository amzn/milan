package com.amazon.milan

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.net.URL
import java.nio.file.{Files, Path}


package object tools {
  private lazy val logger = Logger(LoggerFactory.getLogger("milan"))

  def addToSbtClasspath(paths: Seq[Path]): Unit = {
    val urls = paths.map(_.toUri.toURL).toList

    urls.foreach(url => logger.info(s"Adding {$url} to classpath."))

    val classLoader = this.getClass.getClassLoader
    val addMethod = classLoader.getClass.getDeclaredMethod("add", classOf[Seq[URL]])
    addMethod.invoke(classLoader, urls)
  }

  /**
   * Compiles a Milan application instance that is provided by an [[ApplicationInstanceProvider]] class.
   *
   * @param providerClassName  The name of a class that implements [[ApplicationInstanceProvider]].
   * @param providerParameters Additional parameters to pass to the provider when creating the application instance.
   * @param compilerClassName  The name of a class that implements [[ApplicationInstanceCompiler]].
   * @param compilerParameters Additional parameters to pass to the compiler when compiling the application instance.
   * @param compilerOutputs    Output types and their associated paths.
   */
  def compileApplicationInstance(providerClassName: String,
                                 providerParameters: List[(String, String)],
                                 compilerClassName: String,
                                 compilerParameters: List[(String, String)],
                                 compilerOutputs: List[(String, String)]): Unit = {
    val providerClass = ClassHelper.loadClass(providerClassName)

    val provider =
      providerClass.getConstructors.find(_.getParameterCount == 0) match {
        case None =>
          throw new Exception(s"Provider class $providerClassName does not have a default constructor.")

        case Some(constructor) =>
          constructor.newInstance().asInstanceOf[ApplicationInstanceProvider]
      }

    val instanceParams = new ParameterListInstanceParameters(providerParameters)
    val instance = provider.getApplicationInstance(instanceParams)

    val actualCompilerClassName = KnownCompilers.convertFromKnownCompiler(compilerClassName)
    val compilerClass = ClassHelper.loadClass(actualCompilerClassName)

    val compiler = compilerClass.getConstructors.find(_.getParameterCount == 0) match {
      case None =>
        throw new Exception(s"Compiler class $actualCompilerClassName does not have a default constructor.")

      case Some(constructor) =>
        constructor.newInstance().asInstanceOf[ApplicationInstanceCompiler]
    }

    val outputs = new NameValueListCompilerOutputs(compilerOutputs)

    outputs.getAllOutputs.foreach { case (name, path) =>
      println(s"Ensuring parent directory exists for output '$name'='$path'")
      Files.createDirectories(path.getParent)
    }

    val compilerInstanceParams = new ParameterListInstanceParameters(compilerParameters)
    compiler.compile(instance, compilerInstanceParams, outputs)
  }
}
