package com.amazon.milan.lang.internal

import com.amazon.milan.lang.ObjectStream
import com.amazon.milan.program.internal.FilteredStreamHost
import com.amazon.milan.types.Record

import scala.reflect.macros.whitebox


/**
 * Macro bundle for operations on [[ObjectStream]] objects.
 *
 * @param c The macro context.
 */
class ObjectStreamMacros(val c: whitebox.Context) extends FilteredStreamHost {

  import c.universe._

  /**
   * Creates an [[ObjectStream]] that is the result of applying a filter operation.
   *
   * @param predicate The filter predicate expression.
   * @tparam T The type of the stream.
   * @return An [[ObjectStream]] representing the filtered stream.
   */
  def where[T <: Record : c.WeakTypeTag](predicate: c.Expr[T => Boolean]): c.Expr[ObjectStream[T]] = {
    val nodeTree = this.createdFilteredStream[T](predicate)
    val tree = q"new ${weakTypeOf[ObjectStream[T]]}($nodeTree)"
    c.Expr[ObjectStream[T]](tree)
  }
}
