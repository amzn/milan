package com.amazon.milan.flink.application


class ApplicationDeserializationException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) {
    this(message, null)
  }
}
