package com.amazon.milan

import java.io.{File, FileOutputStream}
import java.net.URL
import java.nio.file.{Files, Path}

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


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
   * @param outputFile         The path to the file where the compiler output will be written.
   * @return A [[File]] pointing to the output file that was written.
   */
  def compileApplicationInstance(providerClassName: String,
                                 providerParameters: List[(String, String)],
                                 compilerClassName: String,
                                 compilerParameters: List[(String, String)],
                                 outputFile: Path): File = {
    val providerClass = ClassHelper.loadClass(providerClassName)

    val provider =
      providerClass.getConstructors.find(_.getParameterCount == 0) match {
        case None =>
          throw new Exception(s"Provider class $providerClassName does not have a default constructor.")

        case Some(constructor) =>
          constructor.newInstance().asInstanceOf[ApplicationInstanceProvider]
      }

    val instance = provider.getApplicationInstance(providerParameters)

    val actualCompilerClassName = KnownCompilers.convertFromKnownCompiler(compilerClassName)
    val compilerClass = ClassHelper.loadClass(actualCompilerClassName)

    val compiler = compilerClass.getConstructors.find(_.getParameterCount == 0) match {
      case None =>
        throw new Exception(s"Compiler class $actualCompilerClassName does not have a default constructor.")

      case Some(constructor) =>
        constructor.newInstance().asInstanceOf[ApplicationInstanceCompiler]
    }

    println(s"Writing generated code to output file '$outputFile'.")

    Files.createDirectories(outputFile.getParent)

    val outputStream = new FileOutputStream(outputFile.toFile)

    try {
      compiler.compile(instance, compilerParameters, outputStream)
      outputFile.toFile
    }
    finally {
      outputStream.close()
    }
  }
}
