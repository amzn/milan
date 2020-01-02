package com.amazon.milan.control

import java.text.SimpleDateFormat

import com.amazon.milan.storage.aws.S3ClientFactory
import org.junit.Assert.assertEquals
import org.junit.Test
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.{mock, when}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable

import scala.collection.JavaConverters._


@Test
class TestSnapshotUtil {

  @Test
  def test_getLatestSnapshotPath_ForAnyDate_WithNoSnapshot_ReturnNone(): Unit = {
    val bucketName = "bucket"
    val baseSnapshotFolder = "base/snapshot/folder"

    // Empty Object listing without any snapshots
    val listObjectsResponse = this.createListObjectsIterable()

    val mockClientFactory = this.createMockS3ClientFactory(bucketName, baseSnapshotFolder, listObjectsResponse)

    val actualPath = SnapshotUtil.getLatestSnapshotPath(mockClientFactory, bucketName, baseSnapshotFolder, None)
    assertEquals(None, actualPath)
  }

  @Test
  def test_getLatestSnapshotPath_ForAnyDate_WithOneSnapshot_ReturnThat(): Unit = {
    val bucketName = "bucket"
    val baseSnapshotFolder = "base/snapshot/folder"

    // List of files created for one snapshot
    val listObjectsResponse = this.createListObjectsIterable(
      s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder_$$folder$$",
      s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/_metadata",
      s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-1",
      s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-2"
    )

    val mockClientFactory = this.createMockS3ClientFactory(bucketName, baseSnapshotFolder, listObjectsResponse)

    val expectedPath = Some("s3://bucket/base/snapshot/folder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/_metadata")
    val actualPath = SnapshotUtil.getLatestSnapshotPath(mockClientFactory, bucketName, baseSnapshotFolder, None)
    assertEquals(expectedPath, actualPath)
  }

  @Test
  def test_getLatestSnapshotPath_ForAnyDate_WithTwoSnapshots_ReturnLatest(): Unit = {
    val bucketName = "bucket"
    val baseSnapshotFolder = "base/snapshot/folder"

    val listObjectsResponse = this.createListObjectsIterable(
      // List of files created for first snapshot
      s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder_$$folder$$",
      s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/_metadata",
      s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-1",
      s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-2",
      // List of files created for second snapshot
      s"$baseSnapshotFolder/2019-01-03T00:00:00.000Z/flink-savepoint-folder_$$folder$$",
      s"$baseSnapshotFolder/2019-01-03T00:00:00.000Z/flink-savepoint-folder/_metadata",
      s"$baseSnapshotFolder/2019-01-03T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-1",
      s"$baseSnapshotFolder/2019-01-03T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-2"
    )

    val mockClientFactory = this.createMockS3ClientFactory(bucketName, baseSnapshotFolder, listObjectsResponse)

    val expectedPath = Some("s3://bucket/base/snapshot/folder/2019-01-03T00:00:00.000Z/flink-savepoint-folder/_metadata")
    val actualPath = SnapshotUtil.getLatestSnapshotPath(mockClientFactory, bucketName, baseSnapshotFolder, None)
    assertEquals(expectedPath, actualPath)
  }

  @Test
  def test_getLatestSnapshotPath_ForGivenDate_WithNoSnapshotForThatDate_ReturnNone(): Unit = {
    val snapshotDate = Some(new SimpleDateFormat("yyyy-MM-dd").parse("2019-01-01"))
    val bucketName = "bucket"
    val baseSnapshotFolder = "base/snapshot/folder"

    // List of snapshot files created on a different date
    val listObjectsResponse = this.createListObjectsIterable(
      s"$baseSnapshotFolder/2019-01-02T00:00:00.000Z/flink-savepoint-folder_$$folder$$",
      s"$baseSnapshotFolder/2019-01-02T00:00:00.000Z/flink-savepoint-folder/_metadata",
      "$baseSnapshotFolder/2019-01-02T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-1",
      s"$baseSnapshotFolder/2019-01-02T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-2"
    )

    val mockClientFactory = this.createMockS3ClientFactory(bucketName, baseSnapshotFolder, listObjectsResponse)

    val actualPath = SnapshotUtil.getLatestSnapshotPath(mockClientFactory, bucketName, baseSnapshotFolder, snapshotDate)
    assertEquals(None, actualPath)
  }

  @Test
  def test_getLatestSnapshotPath_ForGivenDate_WithOneSnapshotForThatDate_ReturnThat(): Unit = {
    val snapshotDate = Some(new SimpleDateFormat("yyyy-MM-dd").parse("2019-01-02"))
    val bucketName = "bucket"
    val baseSnapshotFolder = "base/snapshot/folder"

    val listObjectsResponse = this.createListObjectsIterable(
      // Snapshot created on given date
      s"$baseSnapshotFolder/2019-01-02T00:00:00.000Z/flink-savepoint-folder_$$folder$$",
      s"$baseSnapshotFolder/2019-01-02T00:00:00.000Z/flink-savepoint-folder/_metadata",
      s"$baseSnapshotFolder/2019-01-02T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-1",
      s"$baseSnapshotFolder/2019-01-02T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-2",
      // Snapshot created on a later date
      s"$baseSnapshotFolder/2019-01-03T07:00:00.000Z/flink-savepoint-folder_$$folder$$",
      s"$baseSnapshotFolder/2019-01-03T07:00:00.000Z/flink-savepoint-folder/_metadata",
      s"$baseSnapshotFolder/2019-01-03T07:00:00.000Z/flink-savepoint-folder/savepoint-data-file-1",
      s"$baseSnapshotFolder/2019-01-03T07:00:00.000Z/flink-savepoint-folder/savepoint-data-file-2"
    )

    val mockClientFactory = this.createMockS3ClientFactory(bucketName, baseSnapshotFolder, listObjectsResponse)

    val actualPath = SnapshotUtil.getLatestSnapshotPath(mockClientFactory, bucketName, baseSnapshotFolder, snapshotDate)
    val expectedPath = Some("s3://bucket/base/snapshot/folder/2019-01-02T00:00:00.000Z/flink-savepoint-folder/_metadata")
    assertEquals(expectedPath, actualPath)
  }

  @Test
  def test_getLatestSnapshotPath_ForGivenDate_WithTwoSnapshotsForThatDate_ReturnLatest(): Unit = {
    val snapshotDate = Some(new SimpleDateFormat("yyyy-MM-dd").parse("2019-01-01"))
    val bucketName = "bucket"
    val baseSnapshotFolder = "base/snapshot/folder"

    val listObjectsResponse = this.createListObjectsIterable(
      // First snapshot on given date
      s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder_$$folder$$",
      "$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/_metadata",
      s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-1",
      s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-2",
      // Second snapshot on given date
      s"$baseSnapshotFolder/2019-01-01T07:00:00.000Z/flink-savepoint-folder_$$folder$$",
      s"$baseSnapshotFolder/2019-01-01T07:00:00.000Z/flink-savepoint-folder/_metadata",
      s"$baseSnapshotFolder/2019-01-01T07:00:00.000Z/flink-savepoint-folder/savepoint-data-file-1",
      s"$baseSnapshotFolder/2019-01-01T07:00:00.000Z/flink-savepoint-folder/savepoint-data-file-2"
    )

    val mockClientFactory = this.createMockS3ClientFactory(bucketName, baseSnapshotFolder, listObjectsResponse)

    val actualPath = SnapshotUtil.getLatestSnapshotPath(mockClientFactory, bucketName, baseSnapshotFolder, snapshotDate)
    val expectedPath = Some("s3://bucket/base/snapshot/folder/2019-01-01T07:00:00.000Z/flink-savepoint-folder/_metadata")
    assertEquals(expectedPath, actualPath)
  }

  private def createListObjectsIterable(keys: String*): ListObjectsV2Iterable = {
    val listObjectsResponse =
      ListObjectsV2Response.builder()
        .contents(keys.map(key => S3Object.builder().key(key).build()).asJavaCollection)
        .build()

    val iterable = mock(classOf[ListObjectsV2Iterable])
    when(iterable.iterator()).thenReturn(List(listObjectsResponse).iterator.asJava)
    iterable
  }

  private def createMockS3ClientFactory(bucketName: String,
                                        baseFolder: String,
                                        listObjectsResponse: ListObjectsV2Iterable): S3ClientFactory = {
    val mockClient = mock(classOf[S3Client])
    when(mockClient.listObjectsV2Paginator(argThat[ListObjectsV2Request](req => req.bucket() == bucketName && req.prefix() == baseFolder)))
      .thenReturn(listObjectsResponse)

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    mockClientFactory
  }
}
