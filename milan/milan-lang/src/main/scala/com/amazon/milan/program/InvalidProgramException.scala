package com.amazon.milan.program


class InvalidProgramException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) {
    this(message, null)
  }
}
