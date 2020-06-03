package com.amazon.milan.typeutil

import org.junit.Assert.assertEquals
import org.junit.Test


@Test
class TestTypeUtil {
  @Test
  def test_TypeUtil_GetGenericArgumentTypeNames_WithThreeBasicTypes_ReturnsExpectedListOfTypeNames(): Unit = {
    val names = getGenericArgumentTypeNames("(Int, String, Float)")
    assertEquals(List("Int", "String", "Float"), names)
  }

  @Test
  def test_TypeUtil_GetGenericArgumentTypeNames_WithNestedGenericTypes_ReturnsExpectedListOfTypeNames(): Unit = {
    val typeName = "Tuple2[Int, com.amazon.milan.test.Tuple3Record[Int, Int, Int]]"
    val genericArgNames = getGenericArgumentTypeNames(typeName)
    assertEquals(List("Int", "com.amazon.milan.test.Tuple3Record[Int, Int, Int]"), genericArgNames)
  }
}
