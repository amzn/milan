package com.amazon.milan.storage

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model._
import org.apache.http.client.methods.HttpRequestBase
import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.{ArgumentMatcher, ArgumentMatchers}


@Test
class TestS3BlobStore {
  @Test
  def test_S3BlobStore_WriteBlob_CallsPutObjectWithBlob(): Unit = {
    val putResult = new PutObjectResult()
    putResult.setETag("etag")

    val mockClient = mock(classOf[AmazonS3])
    when(mockClient.putObject(anyString(), anyString(), any[InputStream], any[ObjectMetadata]))
      .thenReturn(putResult)

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val blobStore = new S3BlobStore(mockClientFactory, "bucket", "folder")
    val blobData = Array[Byte](1, 2, 3)
    blobStore.writeBlob("key", new ByteArrayInputStream(blobData))

    verify(mockClient).putObject(
      ArgumentMatchers.eq("bucket"),
      ArgumentMatchers.eq("folder/key"),
      argThat(new ArgumentMatcher[InputStream] {
        override def matches(arg: InputStream): Boolean = {
          arg.reset()
          val bytes = IO.readAllBytes(arg)
          bytes.sameElements(blobData)
        }
      }),
      argThat(new ArgumentMatcher[ObjectMetadata] {
        override def matches(arg: ObjectMetadata): Boolean = arg.getContentLength == 3
      }))
  }

  @Test
  def test_S3BlobStore_ReadBlob_WritesExpectedBlobBytesToOutputStream(): Unit = {
    val blobData = Array[Byte](1, 2, 3)
    val inputStream = new ByteArrayInputStream(blobData)
    val request = mock(classOf[HttpRequestBase])

    val objectStream = new S3ObjectInputStream(inputStream, request)

    val mockObject = mock(classOf[S3Object])
    when(mockObject.getObjectContent).thenReturn(objectStream)

    val mockClient = mock(classOf[AmazonS3])
    when(mockClient.getObject(any[GetObjectRequest])).thenReturn(mockObject)

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val blobStore = new S3BlobStore(mockClientFactory, "bucket", "folder")

    val outputStream = new ByteArrayOutputStream()
    blobStore.readBlob("key", outputStream)

    assertArrayEquals(blobData, outputStream.toByteArray)

    verify(mockClient).getObject(
      argThat(new ArgumentMatcher[GetObjectRequest] {
        override def matches(r: GetObjectRequest): Boolean = {
          r.getBucketName == "bucket" && r.getKey == "folder/key"
        }
      })
    )
  }
}
