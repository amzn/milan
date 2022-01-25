package com.amazon.milan.tools

import java.nio.file.{Path, Paths}

trait CompilerOutputs {
  def getOutput(name: String): Path = {
    this.getAllOutputs.find { case (outputName, _) => outputName == name } match {
      case Some((_, value)) =>
        value

      case None =>
        throw new IllegalArgumentException(s"No output named '$name' exists.'")
    }
  }

  def getAllOutputs: List[(String, Path)]
}


class NameValueListCompilerOutputs(outputs: List[(String, String)]) extends CompilerOutputs {
  private val paths = outputs.map { case (name, value) => (name, Paths.get(value)) }

  if (outputs.map { case (name, _) => name }.toSet.size < outputs.size) {
    throw new IllegalArgumentException(s"Found duplicate output types.")
  }

  override def getAllOutputs: List[(String, Path)] =
    this.paths
}
