package com.amazon.milan

import scala.language.experimental.macros
import scala.language.implicitConversions


package object lang {
  def fields[T](field1: FieldExpression[T]): Tuple1[T] = throw new NotImplementedError()

  def fields[T1, T2](field1: FieldExpression[T1], field2: FieldExpression[T2]): (T1, T2) = throw new NotImplementedError()

  def fields[T1, T2, T3](field1: FieldExpression[T1],
                         field2: FieldExpression[T2],
                         field3: FieldExpression[T3]): (T1, T2, T3) = throw new NotImplementedError()

  def fields[T1, T2, T3, T4](field1: FieldExpression[T1],
                             field2: FieldExpression[T2],
                             field3: FieldExpression[T3],
                             field4: FieldExpression[T4]): (T1, T2, T3, T4) = throw new NotImplementedError()

  def fields[T1, T2, T3, T4, T5](field1: FieldExpression[T1],
                                 field2: FieldExpression[T2],
                                 field3: FieldExpression[T3],
                                 field4: FieldExpression[T4],
                                 field5: FieldExpression[T5]): (T1, T2, T3, T4, T5) = throw new NotImplementedError()

  /**
   * Defines a named field with the specified value.
   */
  def field[T](name: String, value: T): FieldExpression[T] = FieldExpression(name, value)
}
