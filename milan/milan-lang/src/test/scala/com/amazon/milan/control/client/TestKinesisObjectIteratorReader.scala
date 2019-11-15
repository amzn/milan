package com.amazon.milan.control.client

import java.nio.ByteBuffer

import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazonaws.services.kinesis.model.Record
import org.junit.Assert._
import org.junit.Test


object TestKinesisObjectIteratorReader {

  case class IntRecord(value: Int)

}

import com.amazon.milan.control.client.TestKinesisObjectIteratorReader._


@Test
class TestKinesisObjectIteratorReader {
  private val mapper = new ScalaObjectMapper()

  @Test
  def test_KinesisObjectIteratorReader_Next_DeserializesTheRecordData(): Unit = {
    val records = Array(
      Some(this.createRecord(IntRecord(5)))
    )

    val reader = new KinesisIteratorObjectReader[IntRecord](records.iterator)

    assertEquals(5, reader.next().get.value)
  }

  private def createRecord[T](content: T): Record = {
    val record = new Record()
    val contentBytes = this.mapper.writeValueAsBytes(content)
    record.setData(ByteBuffer.wrap(contentBytes))
    record
  }
}
