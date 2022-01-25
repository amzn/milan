package com.amazon.milan.tools

import com.amazon.milan.cmd.{ArgumentsBase, NamedArgument, ParametersArgument}

import java.nio.file.Paths
import scala.collection.mutable


object CompileApplicationInstance {

  class CmdArgs extends ArgumentsBase {
    @NamedArgument(Name = "provider", ShortName = "p", Required = true)
    var providerClassName: String = _

    @NamedArgument(Name = "compiler", ShortName = "c", Required = true)
    var compilerClassName: String = _

    @ParametersArgument(Prefix = "P")
    var providerParameters: mutable.ListBuffer[(String, String)] = mutable.ListBuffer.empty

    @ParametersArgument(Prefix = "C")
    var compilerParameters: mutable.ListBuffer[(String, String)] = mutable.ListBuffer.empty

    @ParametersArgument(Prefix = "O")
    var outputs: mutable.ListBuffer[(String, String)] = mutable.ListBuffer.empty
  }

  def main(args: Array[String]): Unit = {
    val params = new CmdArgs
    params.parse(args, allowUnknownArguments = true)

    compileApplicationInstance(
      params.providerClassName,
      params.providerParameters.toList,
      params.compilerClassName,
      params.compilerParameters.toList,
      params.outputs.toList)
  }
}
