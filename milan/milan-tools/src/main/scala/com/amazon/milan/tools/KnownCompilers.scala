package com.amazon.milan.tools


object KnownCompilers {
  private val knownCompilers = Map(
    "flink" -> "com.amazon.milan.flink.Compiler"
  )

  def convertFromKnownCompiler(compilerClassName: String): String = {
    knownCompilers.get(compilerClassName) match {
      case Some(className) => className
      case _ => compilerClassName
    }
  }
}
