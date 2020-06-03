package com.amazon.milan.lang

import com.amazon.milan.program.{ConstantValue, NamedField, NamedFields, Tree}
import org.junit.Test


@Test
class TestNamedFields {
  @Test
  def test_NamedFields_WithTwoFieldsWithConstantValues_ProducesExpectedExpressionTree(): Unit = {
    val tree = Tree.fromExpression(fields(
      field("x", 1),
      field("y", "foo")
    ))

    val NamedFields(List(
    NamedField("x", ConstantValue(1, _)),
    NamedField("y", ConstantValue("foo", _))
    )) = tree
  }
}
