package com.amazon.milan.storage.aws

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.amazon.milan.storage.IO
import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.AbortableInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse, PutObjectRequest, PutObjectResponse}


@Test
class TestS3BlobStore {
  @Test
  def test_S3BlobStore_WriteBlob_CallsPutObjectWithBlob(): Unit = {
    val putResult = PutObjectResponse.builder().eTag("etag").build()

    val mockClient = mock(classOf[S3Client])
    when(mockClient.putObject(any[PutObjectRequest](), any[RequestBody]()))
      .thenReturn(putResult)

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val blobStore = new S3BlobStore(mockClientFactory, "bucket", "folder")
    val blobData = Array[Byte](1, 2, 3)
    blobStore.writeBlob("key", new ByteArrayInputStream(blobData))

    verify(mockClient).putObject(
      argThat[PutObjectRequest](req => req.bucket() == "bucket" && req.key() == "folder/key" && req.contentLength() == 3),
      argThat[RequestBody](body => IO.readAllBytes(body.contentStreamProvider().newStream()).sameElements(blobData)))
  }

  @Test
  def test_S3BlobStore_ReadBlob_WritesExpectedBlobBytesToOutputStream(): Unit = {
    val blobData = Array[Byte](1, 2, 3)
    val inputStream = new ByteArrayInputStream(blobData)

    val mockClient = mock(classOf[S3Client])
    val response = GetObjectResponse.builder().contentLength(blobData.length.toLong).build()
    val stream = AbortableInputStream.create(inputStream)
    val responseStream = new ResponseInputStream[GetObjectResponse](response, stream)
    when(mockClient.getObject(any[GetObjectRequest])).thenReturn(responseStream)

    val mockClientFactory = mock(classOf[S3ClientFactory])
    when(mockClientFactory.createClient()).thenReturn(mockClient)

    val blobStore = new S3BlobStore(mockClientFactory, "bucket", "folder")

    val outputStream = new ByteArrayOutputStream()
    blobStore.readBlob("key", outputStream)

    assertArrayEquals(blobData, outputStream.toByteArray)

    verify(mockClient).getObject(
      argThat[GetObjectRequest](req => req.bucket() == "bucket" && req.key() == "folder/key")
    )
  }
}
