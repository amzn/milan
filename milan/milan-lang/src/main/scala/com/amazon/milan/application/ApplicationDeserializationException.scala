package com.amazon.milan.application

class ApplicationDeserializationException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) {
    this(message, null)
  }
}
