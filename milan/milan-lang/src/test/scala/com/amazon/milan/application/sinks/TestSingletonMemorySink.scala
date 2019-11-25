package com.amazon.milan.application.sinks

import com.amazon.milan.application.DataSink
import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.typeutil.createTypeDescriptor
import org.junit.Assert._
import org.junit.Test


object TestSingletonMemorySink {

  case class Record(i: Int)

}

import com.amazon.milan.application.sinks.TestSingletonMemorySink._


@Test
class TestSingletonMemorySink {
  @Test
  def test_SingletonMemorySink_AfterSerializeAndDeserialize_ReturnsSameItemsAsOriginal(): Unit = {
    val original = new SingletonMemorySink[Record]()
    val originalAsSink = original.asInstanceOf[DataSink[Record]]

    // Serialize and deserialize it as a DataSink[T] so that we are testing the DataSinkSerializer and DataSinkDeserializer.
    val copy = ScalaObjectMapper.copy(originalAsSink)

    SingletonMemorySink.add(original.sinkId, Record(1))
    SingletonMemorySink.add(original.sinkId, Record(2))

    assertEquals(List(Record(1), Record(2)), original.getValues)
    assertEquals(List(Record(1), Record(2)), copy.asInstanceOf[SingletonMemorySink[Record]].getValues)
  }
}
