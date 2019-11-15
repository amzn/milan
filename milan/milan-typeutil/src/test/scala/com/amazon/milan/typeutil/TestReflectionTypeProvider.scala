package com.amazon.milan.typeutil

import org.junit.Assert._
import org.junit.Test


object TestReflectionTypeProvider {

  class TestClass(var i: Int, var s: String, var c: TestChildClass)

  class TestChildClass(var i: Int)

}

import com.amazon.milan.typeutil.TestReflectionTypeProvider._


@Test
class TestReflectionTypeProvider {
  @Test
  def test_ReflectionTypeProvider_GetTypeDescriptor_ReturnsTypeDescriptorEquivalentToCompileTimeMacro(): Unit = {
    val provider = new ReflectionTypeProvider(getClass.getClassLoader)
    val typeDesc = provider.getTypeDescriptor[TestClass]("com.amazon.milan.typeutil.TestReflectionTypeProvider.TestClass", List())
    assertEquals(TypeDescriptor.of[TestClass], typeDesc)
  }
}
