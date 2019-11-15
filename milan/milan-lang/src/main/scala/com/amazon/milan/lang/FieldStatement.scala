package com.amazon.milan.lang


/**
 * Describes how a named field is computed from one input.
 *
 * @param statement The function that computes the field.
 * @param fieldName The name of the field.
 */
case class FieldStatement[TIn, TOut](statement: TIn => TOut, fieldName: String)


/**
 * Describes how a named field is computed from two inputs.
 *
 * @param statement The function that computes the field.
 * @param fieldName The name of the field.
 */
case class Function2FieldStatement[T1, T2, TOut](statement: (T1, T2) => TOut, fieldName: String)


class FunctionExtensions[TIn, TOut](f: TIn => TOut) {
  /**
   * Converts a [[FunctionExtensions]] instance into a [[FieldStatement]].
   *
   * @param name The name of the field.
   * @return A [[FieldStatement]] containing the name of the field and the function that computes the field.
   */
  def as(name: String): FieldStatement[TIn, TOut] = FieldStatement[TIn, TOut](this.f, name)
}


class Function2Extensions[TLeft, TRight, TOut](f: (TLeft, TRight) => TOut) {
  /**
   * Converts a [[Function2Extensions]] instance into a [[Function2FieldStatement]].
   *
   * @param name The name of the field.
   * @return A [[Function2FieldStatement]] containing the name of the field and the function that computes the field.
   */
  def as(name: String): Function2FieldStatement[TLeft, TRight, TOut] = Function2FieldStatement[TLeft, TRight, TOut](this.f, name)
}
