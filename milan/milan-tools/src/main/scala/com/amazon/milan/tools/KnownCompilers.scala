package com.amazon.milan.tools


object KnownCompilers {
  private val knownCompilers = Map(
    "flink" -> "com.amazon.milan.compiler.flink.Compiler",
    "scala_event" -> "com.amazon.milan.compiler.scala.ScalaEventCompiler",
    "scala_stream" -> "com.amazon.milan.compiler.scala.ScalaStreamCompiler",
    "aws_lambda" -> "com.amazon.milan.aws.serverless.compiler.AwsServerlessCompiler",
  )

  def convertFromKnownCompiler(compilerClassName: String): String = {
    knownCompilers.get(compilerClassName) match {
      case Some(className) => className
      case _ => compilerClassName
    }
  }
}
