package com.amazon.milan.flink

import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.flink.api.scala._
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._


object TestRuntimeEvaluator {

  class GenericType[T]

}

import com.amazon.milan.flink.TestRuntimeEvaluator._


@Test
class TestRuntimeEvaluator {
  @Test
  def test_RuntimeEvaluator_CreateTypeInformation_WithGenericType_ReturnsTypeInformationWithGenericArguments(): Unit = {
    val typeDesc = TypeDescriptor.of[GenericType[Int]]
    val ty = RuntimeEvaluator.default.createTypeInformation(typeDesc)
    assertEquals(Map("T" -> createTypeInformation[Int]), ty.getGenericParameters.asScala)
  }
}
