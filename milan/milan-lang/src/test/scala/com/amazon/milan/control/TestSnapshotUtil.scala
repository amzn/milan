package com.amazon.milan.control

import java.text.SimpleDateFormat

import com.amazon.milan.storage.S3ClientFactory
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ObjectListing, S3ObjectSummary}
import org.junit.Assert.assertEquals
import org.junit.Test
import org.mockito.Mockito.{mock, when}

@Test
class TestSnapshotUtil {

  @Test
  def test_getLatestSnapshotPath_ForAnyDate_WithNoSnapshot_ReturnNone(): Unit = {
    val bucketName = "bucket"
    val baseSnapshotFolder = "base/snapshot/folder"

    // Empty Object listing without any snapshots
    val objectListing = new ObjectListing()

    val mockClient = mock(classOf[AmazonS3])
    when(mockClient.listObjects(bucketName, baseSnapshotFolder)).thenReturn(objectListing)

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val actualPath = SnapshotUtil.getLatestSnapshotPath(mockClientFactory, bucketName, baseSnapshotFolder, None)
    assertEquals(None, actualPath)
  }

  @Test
  def test_getLatestSnapshotPath_ForAnyDate_WithOneSnapshot_ReturnThat(): Unit = {
    val bucketName = "bucket"
    val baseSnapshotFolder = "base/snapshot/folder"

    // List of files created for one snapshot
    val objectListing = new ObjectListing()
    val object1 = new S3ObjectSummary()
    object1.setKey(s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder_$$folder$$")
    val object2 = new S3ObjectSummary()
    object2.setKey(s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/_metadata")
    val object3 = new S3ObjectSummary()
    object3.setKey(s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-1")
    val object4 = new S3ObjectSummary()
    object4.setKey(s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-2")
    objectListing.getObjectSummaries.add(object1)
    objectListing.getObjectSummaries.add(object2)
    objectListing.getObjectSummaries.add(object3)
    objectListing.getObjectSummaries.add(object4)

    val mockClient = mock(classOf[AmazonS3])
    when(mockClient.listObjects(bucketName, baseSnapshotFolder)).thenReturn(objectListing)

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val expectedPath = Some("s3://bucket/base/snapshot/folder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/_metadata")
    val actualPath = SnapshotUtil.getLatestSnapshotPath(mockClientFactory, bucketName, baseSnapshotFolder, None)
    assertEquals(expectedPath, actualPath)
  }

  @Test
  def test_getLatestSnapshotPath_ForAnyDate_WithTwoSnapshots_ReturnLatest(): Unit = {
    val bucketName = "bucket"
    val baseSnapshotFolder = "base/snapshot/folder"

    // List of files created for first snapshot
    val objectListing = new ObjectListing()
    val object1 = new S3ObjectSummary()
    object1.setKey(s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder_$$folder$$")
    val object2 = new S3ObjectSummary()
    object2.setKey(s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/_metadata")
    val object3 = new S3ObjectSummary()
    object3.setKey(s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-1")
    val object4 = new S3ObjectSummary()
    object4.setKey(s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-2")
    objectListing.getObjectSummaries.add(object1)
    objectListing.getObjectSummaries.add(object2)
    objectListing.getObjectSummaries.add(object3)
    objectListing.getObjectSummaries.add(object4)

    // List of files created for second snapshot
    val object5 = new S3ObjectSummary()
    object5.setKey(s"$baseSnapshotFolder/2019-01-03T00:00:00.000Z/flink-savepoint-folder_$$folder$$")
    val object6 = new S3ObjectSummary()
    object6.setKey(s"$baseSnapshotFolder/2019-01-03T00:00:00.000Z/flink-savepoint-folder/_metadata")
    val object7 = new S3ObjectSummary()
    object7.setKey(s"$baseSnapshotFolder/2019-01-03T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-1")
    val object8 = new S3ObjectSummary()
    object8.setKey(s"$baseSnapshotFolder/2019-01-03T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-2")
    objectListing.getObjectSummaries.add(object5)
    objectListing.getObjectSummaries.add(object6)
    objectListing.getObjectSummaries.add(object7)
    objectListing.getObjectSummaries.add(object8)

    val mockClient = mock(classOf[AmazonS3])
    when(mockClient.listObjects(bucketName, baseSnapshotFolder)).thenReturn(objectListing)

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

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
    val objectListing = new ObjectListing()
    val object1 = new S3ObjectSummary()
    object1.setKey(s"$baseSnapshotFolder/2019-01-02T00:00:00.000Z/flink-savepoint-folder_$$folder$$")
    val object2 = new S3ObjectSummary()
    object2.setKey(s"$baseSnapshotFolder/2019-01-02T00:00:00.000Z/flink-savepoint-folder/_metadata")
    val object3 = new S3ObjectSummary()
    object3.setKey(s"$baseSnapshotFolder/2019-01-02T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-1")
    val object4 = new S3ObjectSummary()
    object4.setKey(s"$baseSnapshotFolder/2019-01-02T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-2")
    objectListing.getObjectSummaries.add(object1)
    objectListing.getObjectSummaries.add(object2)
    objectListing.getObjectSummaries.add(object3)
    objectListing.getObjectSummaries.add(object4)

    val mockClient = mock(classOf[AmazonS3])
    when(mockClient.listObjects(bucketName, baseSnapshotFolder)).thenReturn(objectListing)

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val actualPath = SnapshotUtil.getLatestSnapshotPath(mockClientFactory, bucketName, baseSnapshotFolder, snapshotDate)
    assertEquals(None, actualPath)
  }

  @Test
  def test_getLatestSnapshotPath_ForGivenDate_WithOneSnapshotForThatDate_ReturnThat(): Unit = {
    val snapshotDate = Some(new SimpleDateFormat("yyyy-MM-dd").parse("2019-01-02"))
    val bucketName = "bucket"
    val baseSnapshotFolder = "base/snapshot/folder"

    // Snapshot created on given date
    val objectListing = new ObjectListing()
    val object1 = new S3ObjectSummary()
    object1.setKey(s"$baseSnapshotFolder/2019-01-02T00:00:00.000Z/flink-savepoint-folder_$$folder$$")
    val object2 = new S3ObjectSummary()
    object2.setKey(s"$baseSnapshotFolder/2019-01-02T00:00:00.000Z/flink-savepoint-folder/_metadata")
    val object3 = new S3ObjectSummary()
    object3.setKey(s"$baseSnapshotFolder/2019-01-02T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-1")
    val object4 = new S3ObjectSummary()
    object4.setKey(s"$baseSnapshotFolder/2019-01-02T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-2")
    objectListing.getObjectSummaries.add(object1)
    objectListing.getObjectSummaries.add(object2)
    objectListing.getObjectSummaries.add(object3)
    objectListing.getObjectSummaries.add(object4)

    // Snapshot created on a later date
    val object5 = new S3ObjectSummary()
    object5.setKey(s"$baseSnapshotFolder/2019-01-03T07:00:00.000Z/flink-savepoint-folder_$$folder$$")
    val object6 = new S3ObjectSummary()
    object6.setKey(s"$baseSnapshotFolder/2019-01-03T07:00:00.000Z/flink-savepoint-folder/_metadata")
    val object7 = new S3ObjectSummary()
    object7.setKey(s"$baseSnapshotFolder/2019-01-03T07:00:00.000Z/flink-savepoint-folder/savepoint-data-file-1")
    val object8 = new S3ObjectSummary()
    object8.setKey(s"$baseSnapshotFolder/2019-01-03T07:00:00.000Z/flink-savepoint-folder/savepoint-data-file-2")
    objectListing.getObjectSummaries.add(object5)
    objectListing.getObjectSummaries.add(object6)
    objectListing.getObjectSummaries.add(object7)
    objectListing.getObjectSummaries.add(object8)

    val mockClient = mock(classOf[AmazonS3])
    when(mockClient.listObjects(bucketName, baseSnapshotFolder)).thenReturn(objectListing)

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val actualPath = SnapshotUtil.getLatestSnapshotPath(mockClientFactory, bucketName, baseSnapshotFolder, snapshotDate)
    val expectedPath = Some("s3://bucket/base/snapshot/folder/2019-01-02T00:00:00.000Z/flink-savepoint-folder/_metadata")
    assertEquals(expectedPath, actualPath)
  }

  @Test
  def test_getLatestSnapshotPath_ForGivenDate_WithTwoSnapshotsForThatDate_ReturnLatest(): Unit = {
    val snapshotDate = Some(new SimpleDateFormat("yyyy-MM-dd").parse("2019-01-01"))
    val bucketName = "bucket"
    val baseSnapshotFolder = "base/snapshot/folder"

    // First snapshot on given date
    val objectListing = new ObjectListing()
    val object1 = new S3ObjectSummary()
    object1.setKey(s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder_$$folder$$")
    val object2 = new S3ObjectSummary()
    object2.setKey(s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/_metadata")
    val object3 = new S3ObjectSummary()
    object3.setKey(s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-1")
    val object4 = new S3ObjectSummary()
    object4.setKey(s"$baseSnapshotFolder/2019-01-01T00:00:00.000Z/flink-savepoint-folder/savepoint-data-file-2")
    objectListing.getObjectSummaries.add(object1)
    objectListing.getObjectSummaries.add(object2)
    objectListing.getObjectSummaries.add(object3)
    objectListing.getObjectSummaries.add(object4)

    // Second snapshot on given date
    val object5 = new S3ObjectSummary()
    object5.setKey(s"$baseSnapshotFolder/2019-01-01T07:00:00.000Z/flink-savepoint-folder_$$folder$$")
    val object6 = new S3ObjectSummary()
    object6.setKey(s"$baseSnapshotFolder/2019-01-01T07:00:00.000Z/flink-savepoint-folder/_metadata")
    val object7 = new S3ObjectSummary()
    object7.setKey(s"$baseSnapshotFolder/2019-01-01T07:00:00.000Z/flink-savepoint-folder/savepoint-data-file-1")
    val object8 = new S3ObjectSummary()
    object8.setKey(s"$baseSnapshotFolder/2019-01-01T07:00:00.000Z/flink-savepoint-folder/savepoint-data-file-2")
    objectListing.getObjectSummaries.add(object5)
    objectListing.getObjectSummaries.add(object6)
    objectListing.getObjectSummaries.add(object7)
    objectListing.getObjectSummaries.add(object8)

    val mockClient = mock(classOf[AmazonS3])
    when(mockClient.listObjects(bucketName, baseSnapshotFolder)).thenReturn(objectListing)

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val actualPath = SnapshotUtil.getLatestSnapshotPath(mockClientFactory, bucketName, baseSnapshotFolder, snapshotDate)
    val expectedPath = Some("s3://bucket/base/snapshot/folder/2019-01-01T07:00:00.000Z/flink-savepoint-folder/_metadata")
    assertEquals(expectedPath, actualPath)
  }
}
