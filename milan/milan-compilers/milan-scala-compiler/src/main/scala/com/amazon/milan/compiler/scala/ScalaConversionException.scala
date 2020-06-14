package com.amazon.milan.compiler.scala

class ScalaConversionException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) {
    this(message, null)
  }
}
