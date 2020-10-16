package com.amazon.milan.compiler.scala.event


class UnexpectedExpressionException(message: String, cause: Throwable)
  extends Exception(message, cause) {

  def this(message: String) {
    this(message, null)
  }
}
