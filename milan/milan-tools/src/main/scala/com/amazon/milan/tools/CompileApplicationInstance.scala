package com.amazon.milan.tools

import java.nio.file.Paths

import com.amazon.milan.cmd.{ArgumentsBase, NamedArgument, ParametersArgument}

import scala.collection.mutable


object CompileApplicationInstance {

  class CmdArgs extends ArgumentsBase {
    @NamedArgument(Name = "provider", ShortName = "p", Required = true)
    var providerClassName: String = _

    @NamedArgument(Name = "compiler", ShortName = "c", Required = true)
    var compilerClassName: String = _

    @NamedArgument(Name = "output", ShortName = "o", Required = true)
    var outputFileName: String = _

    @ParametersArgument(Prefix = "P")
    var providerParameters: mutable.ListBuffer[(String, String)] = mutable.ListBuffer.empty

    @ParametersArgument(Prefix = "C")
    var compilerParameters: mutable.ListBuffer[(String, String)] = mutable.ListBuffer.empty
  }

  def main(args: Array[String]): Unit = {
    val params = new CmdArgs
    params.parse(args, allowUnknownArguments = true)

    val outputFile = Paths.get(params.outputFileName)

    compileApplicationInstance(
      params.providerClassName,
      params.providerParameters.toList,
      params.compilerClassName,
      params.compilerParameters.toList,
      outputFile)
  }
}
