package com.amazon.milan.flink.types

import com.amazon.milan.flink.testutil.ObjectStreamUtil
import org.junit.Assert._
import org.junit.Test


@Test
class TestArrayRecordTypeInformation {
  @Test
  def test_TupleStreamTypeInformation_CanSerializeAndDeserializeViaObjectStream(): Unit = {
    val original = ArrayRecordTypeInformation.createFromFieldTypes(List(("a", "Int"), ("b", "String")))
    val copy = ObjectStreamUtil.serializeAndDeserialize(original)
    assertEquals(original, copy)
  }
}
