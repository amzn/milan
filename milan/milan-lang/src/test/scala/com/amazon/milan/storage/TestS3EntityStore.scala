package com.amazon.milan.storage

import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ObjectListing, PutObjectResult, S3ObjectSummary}
import org.apache.http.HttpStatus
import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._


object TestS3EntityStore {

  case class IntRecord(value: Int)

}

import com.amazon.milan.storage.TestS3EntityStore._


@Test
class TestS3EntityStore {
  private val objectMapper = new ScalaObjectMapper()

  @Test
  def test_S3EntityStore_GetEntity_DeserializesAndReturnsExpectedValue(): Unit = {
    val value = IntRecord(1)
    val valueJson = this.objectMapper.writeValueAsString(value)

    val mockClient = mock(classOf[AmazonS3])
    when(mockClient.getObjectAsString("bucket", "folder/key.json"))
      .thenReturn(valueJson)

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val store = new S3EntityStore[IntRecord](mockClientFactory, "bucket", "folder", ".json", this.objectMapper)
    val actualValue = store.getEntity("key")

    assertEquals(value, actualValue)
  }

  @Test
  def test_S3EntityStore_EntityExists_ReturnsWhatS3ClientReturns(): Unit = {
    val mockClient = mock(classOf[AmazonS3])
    when(mockClient.doesObjectExist("bucket", "folder/key.json"))
      .thenReturn(false)
      .thenReturn(true)

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val store = new S3EntityStore[IntRecord](mockClientFactory, "bucket", "folder", ".json")

    assertFalse(store.entityExists("key"))
    assertTrue(store.entityExists("key"))
  }

  @Test
  def test_S3EntityStore_PutEntity_CallsS3ClientPutObjectWithJsonObject(): Unit = {
    val value = IntRecord(3)
    val valueJson = this.objectMapper.writeValueAsString(value)

    val mockClient = mock(classOf[AmazonS3])
    when(mockClient.putObject("bucket", "folder/key.json", valueJson))
      .thenReturn(new PutObjectResult())

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val store = new S3EntityStore[IntRecord](mockClientFactory, "bucket", "folder", ".json", this.objectMapper)

    store.putEntity("key", value)

    verify(mockClient).putObject("bucket", "folder/key.json", valueJson)
  }

  @Test
  def test_S3EntityStore_ListEntityKeys_WithTwoBatches_ReturnsAllObjectKeys(): Unit = {
    val batch1 = new ObjectListing()
    val object1 = new S3ObjectSummary()
    object1.setKey("folder/key1.json")
    batch1.getObjectSummaries.add(object1)
    batch1.setTruncated(true)

    val batch2 = new ObjectListing()
    val object2 = new S3ObjectSummary()
    object2.setKey("folder/key2.json")
    batch2.getObjectSummaries.add(object2)
    batch2.setTruncated(false)

    val mockClient = mock(classOf[AmazonS3])
    when(mockClient.listObjects("bucket", "folder")).thenReturn(batch1)
    when(mockClient.listNextBatchOfObjects(any[ObjectListing])).thenReturn(batch2)

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val store = new S3EntityStore[IntRecord](mockClientFactory, "bucket", "folder", ".json", this.objectMapper)

    val keys = store.listEntityKeys().map(_.asInstanceOf[AnyRef]).toArray
    assertArrayEquals(Array[AnyRef]("key1", "key2"), keys)
  }

  @Test
  def test_S3EntityStore_ListEntities_WithTwoEntities_ReturnsBothDeserializedEntities(): Unit = {
    val batch = new ObjectListing()
    batch.setTruncated(false)

    val object1 = new S3ObjectSummary()
    object1.setKey("folder/key1.json")
    batch.getObjectSummaries.add(object1)

    val object2 = new S3ObjectSummary()
    object2.setKey("folder/key2.json")
    batch.getObjectSummaries.add(object2)

    val mockClient = mock(classOf[AmazonS3])
    when(mockClient.listObjects("bucket", "folder")).thenReturn(batch)

    val value1 = IntRecord(1)
    val value1Json = this.objectMapper.writeValueAsString(value1)
    val value2 = IntRecord(2)
    val value2Json = this.objectMapper.writeValueAsString(value2)
    when(mockClient.getObjectAsString("bucket", "folder/key1.json")).thenReturn(value1Json)
    when(mockClient.getObjectAsString("bucket", "folder/key2.json")).thenReturn(value2Json)

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val store = new S3EntityStore[IntRecord](mockClientFactory, "bucket", "folder", ".json", this.objectMapper)

    val values = store.listEntities().map(_.asInstanceOf[AnyRef]).toArray
    val expectedValues = Array[AnyRef](value1, value2)
    assertArrayEquals(expectedValues, values)
  }

  @Test(expected = classOf[EntityNotFoundException])
  def test_S3EntityStore_GetEntity_WhenEntityDoesNotExist_ThrowsEntityNotFoundException(): Unit = {
    val mockClient = mock(classOf[AmazonS3])
    when(mockClient.getObjectAsString(anyString(), anyString()))
      .thenThrow(S3ExceptionHelper.create(HttpStatus.SC_NOT_FOUND))

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val store = new S3EntityStore[IntRecord](mockClientFactory, "bucket", "folder", ".json", this.objectMapper)
    store.getEntity("key")
  }
}
