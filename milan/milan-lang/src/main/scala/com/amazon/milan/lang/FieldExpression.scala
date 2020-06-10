package com.amazon.milan.lang


/**
 * Represents a named field in a Milan expression.
 * This class is used to inform the Milan Scala DSL interpreter that a NamedField expression should be created from the
 * Scala AST of the field value.
 *
 * @param name  The name of the field.
 * @param value The value of the field.
 * @tparam T The type of the field.
 */
case class FieldExpression[T](name: String, value: T) {
}
