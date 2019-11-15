package com.amazon.milan.typeutil

class UnsupportedTypeException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) {
    this(message, null)
  }
}
