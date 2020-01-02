package com.amazon.milan.storage.aws

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.storage.{EntityNotFoundException, IO}
import org.apache.http.HttpStatus
import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.core.{ResponseBytes, ResponseInputStream}
import software.amazon.awssdk.http.AbortableInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable

import scala.collection.JavaConverters._


object TestS3EntityStore {

  case class IntRecord(value: Int)

}

import com.amazon.milan.storage.aws.TestS3EntityStore._


@Test
class TestS3EntityStore {
  private val objectMapper = new ScalaObjectMapper()

  @Test
  def test_S3EntityStore_GetEntity_DeserializesAndReturnsExpectedValue(): Unit = {
    val value = IntRecord(1)
    val valueJson = this.objectMapper.writeValueAsString(value)

    val mockClient = mock(classOf[S3Client])
    this.addObjectAsBytes(mockClient, "bucket", "folder/key.json", valueJson)

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val store = new S3EntityStore[IntRecord](mockClientFactory, "bucket", "folder", ".json", this.objectMapper)
    val actualValue = store.getEntity("key")

    assertEquals(value, actualValue)
  }

  @Test
  def test_S3EntityStore_EntityExists_ReturnsWhatS3ClientReturns(): Unit = {
    val mockClient = mock(classOf[S3Client])

    val response = GetObjectResponse.builder().build()
    val stream = AbortableInputStream.create(new ByteArrayInputStream(Array.empty))

    when(mockClient.getObject(argThat[GetObjectRequest](req => req.bucket() == "bucket" && req.key() == "folder/key.json")))
      .thenReturn(new ResponseInputStream[GetObjectResponse](response, stream))
      .thenThrow(S3ExceptionHelper.create(HttpStatus.SC_NOT_FOUND))

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val store = new S3EntityStore[IntRecord](mockClientFactory, "bucket", "folder", ".json")

    assertTrue(store.entityExists("key"))
    assertFalse(store.entityExists("key"))
  }

  @Test
  def test_S3EntityStore_PutEntity_CallsS3ClientPutObjectWithJsonObject(): Unit = {
    val value = IntRecord(3)
    val valueJson = this.objectMapper.writeValueAsString(value)

    val mockClient = mock(classOf[S3Client])
    when(mockClient.putObject(
      argThat[PutObjectRequest](req => req.bucket() == "bucket" && req.key() == "folder/key.json"),
      argThat[RequestBody](body => IO.readAllString(body.contentStreamProvider().newStream()) == valueJson)))
      .thenReturn(PutObjectResponse.builder().build())

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val store = new S3EntityStore[IntRecord](mockClientFactory, "bucket", "folder", ".json", this.objectMapper)

    store.putEntity("key", value)

    verify(mockClient).putObject(
      argThat[PutObjectRequest](req => req.bucket() == "bucket" && req.key == "folder/key.json"),
      argThat[RequestBody](body => IO.readAllString(body.contentStreamProvider().newStream()) == valueJson))
  }

  @Test
  def test_S3EntityStore_ListEntityKeys_WithTwoBatches_ReturnsAllObjectKeys(): Unit = {
    val mockClient = mock(classOf[S3Client])
    this.addObjectListing(mockClient, "bucket", "folder", List(List("folder/key1.json"), List("folder/key2.json")))

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val store = new S3EntityStore[IntRecord](mockClientFactory, "bucket", "folder", ".json", this.objectMapper)

    val keys = store.listEntityKeys().map(_.asInstanceOf[AnyRef]).toArray
    assertArrayEquals(Array[AnyRef]("key1", "key2"), keys)
  }

  @Test
  def test_S3EntityStore_ListEntities_WithTwoEntities_ReturnsBothDeserializedEntities(): Unit = {
    val mockClient = mock(classOf[S3Client])
    this.addObjectListing(mockClient, "bucket", "folder", List(List("folder/key1.json", "folder/key2.json")))

    val value1 = IntRecord(1)
    val value1Json = this.objectMapper.writeValueAsString(value1)
    this.addObjectAsBytes(mockClient, "bucket", "folder/key1.json", value1Json)

    val value2 = IntRecord(2)
    val value2Json = this.objectMapper.writeValueAsString(value2)
    this.addObjectAsBytes(mockClient, "bucket", "folder/key2.json", value2Json)

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val store = new S3EntityStore[IntRecord](mockClientFactory, "bucket", "folder", ".json", this.objectMapper)

    val values = store.listEntities().map(_.asInstanceOf[AnyRef]).toArray
    val expectedValues = Array[AnyRef](value1, value2)
    assertArrayEquals(expectedValues, values)
  }

  @Test(expected = classOf[EntityNotFoundException])
  def test_S3EntityStore_GetEntity_WhenEntityDoesNotExist_ThrowsEntityNotFoundException(): Unit = {
    val mockClient = mock(classOf[S3Client])
    when(mockClient.getObjectAsBytes(any[GetObjectRequest]()))
      .thenThrow(S3ExceptionHelper.create(HttpStatus.SC_NOT_FOUND))

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val store = new S3EntityStore[IntRecord](mockClientFactory, "bucket", "folder", ".json", this.objectMapper)
    store.getEntity("key")
  }

  private def addObjectListing(client: S3Client, bucket: String, prefix: String, batches: List[List[String]]): Unit = {
    val responses = batches.map(keys =>
      ListObjectsV2Response.builder().contents(keys.map(key => S3Object.builder().key(key).build()).asJavaCollection).build()
    )

    val iterable = mock(classOf[ListObjectsV2Iterable])
    when(iterable.iterator()).thenReturn(responses.iterator.asJava)

    when(client.listObjectsV2Paginator(argThat[ListObjectsV2Request](req => req.bucket() == bucket && req.prefix() == prefix)))
      .thenReturn(iterable)
  }

  private def addObjectAsBytes(client: S3Client, bucket: String, key: String, objectString: String): Unit = {
    val bytes = objectString.getBytes(StandardCharsets.UTF_8)
    val response = GetObjectResponse.builder().contentLength(bytes.length.toLong).build()
    val responseBytes = ResponseBytes.fromByteArray[GetObjectResponse](response, bytes)
    when(client.getObjectAsBytes(argThat[GetObjectRequest](req => req != null && req.bucket() == bucket && req.key() == key)))
      .thenReturn(responseBytes)
  }
}
