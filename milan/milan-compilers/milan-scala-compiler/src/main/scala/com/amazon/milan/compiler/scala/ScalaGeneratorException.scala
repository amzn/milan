package com.amazon.milan.compiler.scala

class ScalaGeneratorException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) {
    this(message, null)
  }
}
