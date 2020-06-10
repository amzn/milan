package com.amazon.milan.typeutil


/**
 * An exception indicating that a type was used that is not supported by Milan.
 */
class UnsupportedTypeException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) {
    this(message, null)
  }
}
