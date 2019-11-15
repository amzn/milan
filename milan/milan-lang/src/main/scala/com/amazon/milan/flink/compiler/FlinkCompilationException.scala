package com.amazon.milan.flink.compiler

class FlinkCompilationException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) {
    this(message, null)
  }
}
