package com.amazon.milan.program

import org.junit.Assert._
import org.junit.Test


@Test
class TestTreeExtensions {
  @Test
  def test_TreeExtensions_ListTermNames_WithOneSelectFieldNode_ReturnsOneName(): Unit = {
    val expr = SelectField(SelectTerm("foo"), "bar")
    val names = expr.listTermNames().toList
    assertEquals(List("foo"), names)
  }
}
