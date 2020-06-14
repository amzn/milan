package com.amazon.milan.compiler.flink.types

import com.amazon.milan.compiler.flink.testutil.ObjectStreamUtil
import org.apache.flink.api.scala._
import org.junit.Assert._
import org.junit.Test


@Test
class TestArrayRecordTypeInformation {
  @Test
  def test_ArrayRecordTypeInformation_CanSerializeAndDeserializeViaObjectStream(): Unit = {
    val original = new ArrayRecordTypeInformation(Array(
      FieldTypeInformation("a", createTypeInformation[Int]),
      FieldTypeInformation("b", createTypeInformation[String])))
    val copy = ObjectStreamUtil.serializeAndDeserialize(original)
    assertEquals(original, copy)
  }
}
