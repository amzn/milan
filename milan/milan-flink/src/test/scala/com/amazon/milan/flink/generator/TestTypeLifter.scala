package com.amazon.milan.flink.generator

import com.amazon.milan.typeutil.TypeDescriptor
import org.junit.Assert._
import org.junit.Test


object TestTypeLifter {

  class GenericClass[T]

}

import com.amazon.milan.flink.generator.TestTypeLifter._


@Test
class TestTypeLifter {
  @Test
  def test_TypeLifter_NameOf_GenericClass_DoesNotIncludeGenericArguments(): Unit = {
    val typeLifter = new TypeLifter()
    assertEquals("com.amazon.milan.flink.generator.TestTypeLifter.GenericClass", typeLifter.nameOf[GenericClass[String]].value)
  }

  @Test
  def test_TypeLifter_LiftTypeDescriptorToTypeInformation_OfTupleType_CreatesScalaTupleTypeInfo(): Unit = {
    val typeDescriptor = TypeDescriptor.of[(Int, String)]
    val typeInfoCode = new TypeLifter().liftTypeDescriptorToTypeInformation(typeDescriptor).value
    assertEquals("new com.amazon.milan.flink.types.ScalaTupleTypeInformation[Tuple2[Int, String]](Array(org.apache.flink.api.scala.createTypeInformation[Int], org.apache.flink.api.scala.createTypeInformation[String]))", typeInfoCode)
  }
}
