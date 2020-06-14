package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.typeutil.TypeDescriptor
import org.junit.Assert._
import org.junit.Test


@Test
class TestTypeLifter {
  @Test
  def test_FlinkTypeLifter_LiftTypeDescriptorToTypeInformation_OfTupleType_CreatesScalaTupleTypeInfo(): Unit = {
    val typeDescriptor = TypeDescriptor.of[(Int, String)]
    val typeInfoCode = new FlinkTypeLifter().liftTypeDescriptorToTypeInformation(typeDescriptor).value
    assertEquals("new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple2[Int, String]](Array(org.apache.flink.api.scala.createTypeInformation[Int], org.apache.flink.api.scala.createTypeInformation[String]))", typeInfoCode)
  }
}
