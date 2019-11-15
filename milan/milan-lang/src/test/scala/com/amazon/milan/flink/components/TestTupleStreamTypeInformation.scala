package com.amazon.milan.flink.components

import org.junit.Assert._
import org.junit.Test


@Test
class TestTupleStreamTypeInformation {
  @Test
  def test_TupleStreamTypeInformation_CanSerializeAndDeserializeViaObjectStream(): Unit = {
    val original = TupleStreamTypeInformation.createFromFieldTypes(List(("a", "Int"), ("b", "String")))
    val copy = ObjectStreamUtil.serializeAndDeserialize(original)
    assertEquals(original, copy)
  }
}
