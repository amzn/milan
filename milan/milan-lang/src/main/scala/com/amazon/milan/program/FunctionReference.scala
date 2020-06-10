package com.amazon.milan.program

import scala.language.experimental.macros


/**
 * A reference to a static function in user code.
 *
 * @param objectTypeName The full name of the object which contains the static function.
 * @param functionName   The name of the function being called.
 */
case class FunctionReference(objectTypeName: String, functionName: String) {
  override def equals(obj: Any): Boolean = obj match {
    case FunctionReference(o, f) => this.objectTypeName.equals(o) && this.functionName.equals(f)
    case _ => false
  }

  override def toString: String = s"""FunctionReference("${this.objectTypeName}", "${this.functionName}")"""
}
