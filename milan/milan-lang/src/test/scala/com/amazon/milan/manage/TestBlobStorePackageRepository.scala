package com.amazon.milan.manage

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.amazon.milan.storage.MemoryBlobStore
import org.junit.Assert._
import org.junit.Test


@Test
class TestBlobStorePackageRepository {
  @Test
  def test_BlobStorePackageRepository_CopyToStream_AfterCopyingToRepository_CopiesOriginalContentsToOutputStream(): Unit = {
    val (repo, _) = this.createRepo()

    val contents = Array[Byte](1, 2, 3, 4)
    repo.copyToRepository("packageId", new ByteArrayInputStream(contents))

    val outputStream = new ByteArrayOutputStream()
    repo.copyToStream("packageId", outputStream)

    assertArrayEquals(contents, outputStream.toByteArray)
  }

  private def createRepo(): (BlobStorePackageRepository, MemoryBlobStore) = {
    val blobStore = new MemoryBlobStore()
    val repo = new BlobStorePackageRepository(blobStore)
    (repo, blobStore)
  }
}
