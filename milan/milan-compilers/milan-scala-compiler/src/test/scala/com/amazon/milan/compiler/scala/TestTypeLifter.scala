package com.amazon.milan.compiler.scala

import org.junit.Assert._
import org.junit.Test


object TestTypeLifter {

  class GenericClass[T]

}

import com.amazon.milan.compiler.scala.TestTypeLifter._


@Test
class TestTypeLifter {
  @Test
  def test_TypeLifter_NameOf_GenericClass_DoesNotIncludeGenericArguments(): Unit = {
    val typeLifter = new TypeLifter()
    assertEquals("com.amazon.milan.compiler.scala.TestTypeLifter.GenericClass", typeLifter.nameOf[GenericClass[String]].value)
  }
}
