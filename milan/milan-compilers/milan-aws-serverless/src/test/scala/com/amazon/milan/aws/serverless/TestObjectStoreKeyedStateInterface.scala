package com.amazon.milan.aws.serverless

import org.junit.Assert._
import org.junit.Test


class EmptyObjectStore extends ObjectStore[Int, Int] {
  override def getItem(key: Int): Option[Int] = None

  override def putItem(key: Int, item: Int): Unit = {}
}

@Test
class TestObjectStoreKeyedStateInterface {
  @Test
  def test_ObjectStoreKeyedStateInterface_GetState_WithMissingItem_ReturnsDefaultValue(): Unit = {
    val objectStore = new EmptyObjectStore()
    val stateInterface = new ObjectStoreKeyedStateInterface[Int, Int](objectStore, -1)

    assertEquals(None, objectStore.getItem(1))
    assertEquals(Some(-1), stateInterface.getState(1))
  }
}
