package com.amazon.milan.control.aws

import com.amazon.milan.serialization.ScalaObjectMapper
import org.junit.Assert._
import org.junit.Test
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.Record


object TestKinesisObjectIteratorReader {

  case class IntRecord(value: Int)

}

import com.amazon.milan.control.aws.TestKinesisObjectIteratorReader._


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
    val contentBytes = this.mapper.writeValueAsBytes(content)
    Record.builder().data(SdkBytes.fromByteArray(contentBytes)).build()
  }
}
