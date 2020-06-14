package com.amazon.milan.compiler.flink.generator


class FlinkGeneratorException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) {
    this(message, null)
  }
}
