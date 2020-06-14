package com.amazon.milan.application.sources

import com.amazon.milan.dataformats.CsvDataInputFormat
import org.junit.Assert.assertEquals
import org.junit.Test


object TestS3DataSource {

  case class IntRecord(value: Int)

}

import com.amazon.milan.application.sources.TestS3DataSource._


@Test
class TestS3DataSource {

  @Test
  def test_S3DataSource_PathIsValid(): Unit = {
    val dataFormat = new CsvDataInputFormat[IntRecord](Array("value"))

    var s3DataSource = new S3DataSource[IntRecord]("exampleBucket", "some/file/key", dataFormat)
    assertEquals("s3://exampleBucket/some/file/key", s3DataSource.path)

    s3DataSource = new S3DataSource[IntRecord]("exampleBucket/", "/some/file/key", dataFormat)
    assertEquals("s3://exampleBucket/some/file/key", s3DataSource.path)
  }
}