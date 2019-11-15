package com.amazon.milan.experimentation

import java.io.ByteArrayInputStream
import java.time.{LocalDateTime, ZoneOffset}

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ObjectListing, S3Object, S3ObjectInputStream, S3ObjectSummary}
import org.apache.http.client.methods.HttpRequestBase
import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.{mock, when}


object TestDatasets {

  case class Record(i: Int)

}

import com.amazon.milan.experimentation.TestDatasets._


@Test
class TestDatasets {
  @Test
  def test_Datasets_ReadS3Files_WithMultipleListingsWithMultipleRecordsInEachFile_ReturnsAllRecords(): Unit = {
    val bucket = "bucket"
    val baseFolder = "base/folder"

    val firstObjectListing = this.createObjectListing(true, s"$baseFolder/a", s"$baseFolder/b")
    val secondObjectListing = this.createObjectListing(false, s"$baseFolder/c")

    val mockClient = mock(classOf[AmazonS3])
    when(mockClient.listObjects(bucket, baseFolder))
      .thenReturn(firstObjectListing)

    when(mockClient.listNextBatchOfObjects(any[ObjectListing]))
      .thenReturn(secondObjectListing)
      .thenThrow(classOf[IllegalStateException])

    val aRecords = this.createObject("""{"i": 1}{"i": 2}""")
    when(mockClient.getObject(bucket, s"$baseFolder/a")).thenReturn(aRecords)

    val bRecords = this.createObject("""{"i": 3}{"i": 4}{"i": 5}""")
    when(mockClient.getObject(bucket, s"$baseFolder/b")).thenReturn(bRecords)

    val cRecords = this.createObject("""{"i": 6}""")
    when(mockClient.getObject(bucket, s"$baseFolder/c")).thenReturn(cRecords)

    val records = Datasets.readS3Files[Record](mockClient, s"s3://$bucket/$baseFolder").toList
    assertEquals(List(Record(1), Record(2), Record(3), Record(4), Record(5), Record(6)), records)
  }

  @Test
  def test_Datasets_ReadS3Files_WithDateRange_ReturnsOnlyRecordsFromFilesInRange(): Unit = {
    val bucket = "bucket"
    val baseFolder = "base/folder"

    val objectListing = this.createObjectListing(
      false,
      s"$baseFolder/foo-2000-01-01-01-02-03-bar",
      s"$baseFolder/foo-2000-01-02-03-04-05-bar",
      s"$baseFolder/foo-2000-01-03-00-00-00-bar")

    val mockClient = mock(classOf[AmazonS3])
    when(mockClient.listObjects(bucket, baseFolder))
      .thenReturn(objectListing)

    val recordsObject = this.createObject("""{"i": 1}{"i": 2}""")
    when(mockClient.getObject(bucket, s"$baseFolder/foo-2000-01-02-03-04-05-bar")).thenReturn(recordsObject)

    val startTime = LocalDateTime.of(2000, 1, 2, 0, 0, 0).toInstant(ZoneOffset.UTC)
    val endTime = LocalDateTime.of(2000, 1, 3, 0, 0, 0).toInstant(ZoneOffset.UTC)
    val records = Datasets.readS3Files[Record](mockClient, s"s3://$bucket/$baseFolder", startTime, endTime).toList
    assertEquals(List(Record(1), Record(2)), records)
  }

  private def createObjectListing(isTruncated: Boolean, keys: String*): ObjectListing = {
    val listing = new ObjectListing()
    listing.setTruncated(isTruncated)
    keys.foreach(key => listing.getObjectSummaries.add(this.createObjectSummary(key)))
    listing
  }

  private def createObjectSummary(key: String): S3ObjectSummary = {
    val obj = new S3ObjectSummary()
    obj.setKey(key)
    obj
  }

  private def createObject(content: String): S3Object = {
    val obj = mock(classOf[S3Object])
    val request = mock(classOf[HttpRequestBase])
    val contentStream = new ByteArrayInputStream(content.getBytes("utf-8"))
    val stream = new S3ObjectInputStream(contentStream, request)
    when(obj.getObjectContent).thenReturn(stream)
    obj
  }
}
