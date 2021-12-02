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

  @Test
  def test_NamedFields_WithFiveFieldsWithConstantValues_ProducesExpectedExpressionTree(): Unit = {
    val tree = Tree.fromExpression(fields(
      field("a", 1),
      field("b", "foo"),
      field("c", 3.0),
      field("d", true),
      field("e", "e")
    ))

    val NamedFields(List(
    NamedField("a", ConstantValue(1, _)),
    NamedField("b", ConstantValue("foo", _)),
    NamedField("c", ConstantValue(3.0, _)),
    NamedField("d", ConstantValue(true, _)),
    NamedField("e", ConstantValue("e", _))
    )) = tree
  }
}
