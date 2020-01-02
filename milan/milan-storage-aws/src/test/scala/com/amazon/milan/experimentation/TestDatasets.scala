package com.amazon.milan.experimentation

import java.time.{LocalDateTime, ZoneOffset}

import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.{mock, when}
import software.amazon.awssdk.core.{ResponseInputStream, SdkBytes}
import software.amazon.awssdk.http.AbortableInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable

import scala.collection.JavaConverters._


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

    val paginator = this.createObjectPaginator(List(s"$baseFolder/a", s"$baseFolder/b"), List(s"$baseFolder/c"))

    val mockClient = mock(classOf[S3Client])
    when(mockClient.listObjectsV2Paginator(argThat[ListObjectsV2Request](req => req.bucket() == bucket && req.prefix() == baseFolder)))
      .thenReturn(paginator)

    this.addObjectResponse(mockClient, bucket, s"$baseFolder/a", """{"i": 1}{"i": 2}""")
    this.addObjectResponse(mockClient, bucket, s"$baseFolder/b", """{"i": 3}{"i": 4}{"i": 5}""")
    this.addObjectResponse(mockClient, bucket, s"$baseFolder/c", """{"i": 6}""")

    val records = Datasets.readS3Files[Record](mockClient, bucket, baseFolder).toList
    assertEquals(List(Record(1), Record(2), Record(3), Record(4), Record(5), Record(6)), records)
  }

  @Test
  def test_Datasets_ReadS3Files_WithDateRange_ReturnsOnlyRecordsFromFilesInRange(): Unit = {
    val bucket = "bucket"
    val baseFolder = "base/folder"

    val paginator = this.createObjectPaginator(
      List(s"$baseFolder/foo-2000-01-01-01-02-03-bar",
        s"$baseFolder/foo-2000-01-02-03-04-05-bar",
        s"$baseFolder/foo-2000-01-03-00-00-00-bar"))

    val mockClient = mock(classOf[S3Client])
    when(mockClient.listObjectsV2Paginator(argThat[ListObjectsV2Request](req => req.bucket() == bucket && req.prefix() == baseFolder)))
      .thenReturn(paginator)

    this.addObjectResponse(mockClient, bucket, s"$baseFolder/foo-2000-01-02-03-04-05-bar", """{"i": 1}{"i": 2}""")

    val startTime = LocalDateTime.of(2000, 1, 2, 0, 0, 0).toInstant(ZoneOffset.UTC)
    val endTime = LocalDateTime.of(2000, 1, 3, 0, 0, 0).toInstant(ZoneOffset.UTC)
    val records = Datasets.readS3Files[Record](mockClient, bucket, baseFolder, startTime, endTime).toList
    assertEquals(List(Record(1), Record(2)), records)
  }

  private def createObjectPaginator(pages: List[String]*): ListObjectsV2Iterable = {
    val paginator = mock(classOf[ListObjectsV2Iterable])
    val responses = pages.map(this.createObjectListing)
    when(paginator.iterator()).thenReturn(responses.iterator.asJava)
    paginator
  }

  private def createObjectListing(keys: String*): ListObjectsV2Response = {
    ListObjectsV2Response.builder()
      .contents(keys.map(key => S3Object.builder().key(key).build()).asJavaCollection)
      .build()
  }

  private def addObjectResponse(client: S3Client, bucket: String, key: String, content: String): Unit = {
    val bytes = SdkBytes.fromUtf8String(content)
    val stream = AbortableInputStream.create(bytes.asInputStream())
    val obj = GetObjectResponse.builder()
      .contentLength(bytes.asByteArray().length.toLong)
      .build()
    val response = new ResponseInputStream[GetObjectResponse](obj, stream)

    when(client.getObject(argThat[GetObjectRequest](req => req != null && req.bucket() == bucket && req.key() == key)))
      .thenReturn(response)
  }
}
