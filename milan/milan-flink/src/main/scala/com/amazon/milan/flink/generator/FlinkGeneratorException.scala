package com.amazon.milan.flink.generator


class FlinkGeneratorException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) {
    this(message, null)
  }
}
