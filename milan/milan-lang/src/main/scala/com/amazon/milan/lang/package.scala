package com.amazon.milan

import scala.language.experimental.macros
import scala.language.implicitConversions


package object lang {
  /**
   * Implicit conversion from an anonymous function of one argument to a [[FunctionExtensions]], which contains the
   * as(name) method allowing users to create a [[FieldStatement]].
   */
  implicit def extendFunction[TIn, TOut](f: TIn => TOut): FunctionExtensions[TIn, TOut] = new FunctionExtensions[TIn, TOut](f)

  /**
   * Implicit conversion from an anonymous function of two arguments to a [[Function2Extensions]], which contains the
   * as(name) method allowing users to create a [[Function2FieldStatement]].
   */
  implicit def extendFunction2[TLeft, TRight, TOut](f: (TLeft, TRight) => TOut): Function2Extensions[TLeft, TRight, TOut] = new Function2Extensions[TLeft, TRight, TOut](f)
}
