package com.amazon.milan.aws.serverless

import com.amazon.milan.compiler.scala.testing.IntRecord
import com.amazon.milan.typeutil.{TypeDescriptor, types}
import org.junit.Assert._
import org.junit.Test
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{GetItemRequest, GetItemResponse, PutItemRequest, PutItemResponse}


@Test
class TestDynamoDbObjectStore {
  @Test
  def test_DynamoDbObjectStore_PutItem_WithSomeOption_StoresOptionValue(): Unit = {
    val client = new TestDynamoDbClient
    val target = DynamoDbObjectStore.create[Int, Option[String]](client, "table", TypeDescriptor.of[Int], TypeDescriptor.of[Option[String]])

    target.putItem(0, Some("test"))

    val item = client.putItems.head
    assertEquals(item.item().get("key").s(), "0")
    val valueJson = item.item().get("value").s()
    assertEquals(valueJson, """"test"""")
  }

  @Test
  def test_DynamoDbObjectStore_PutItem_WithNoneOption_StoresNone(): Unit = {
    val client = new TestDynamoDbClient
    val target = DynamoDbObjectStore.create[Int, Option[String]](client, "table", TypeDescriptor.of[Int], TypeDescriptor.of[Option[String]])

    target.putItem(0, None)

    val item = client.putItems.head
    assertEquals(item.item().get("key").s(), "0")
    val valueJson = item.item().get("value").s()
    assertEquals(valueJson, "null")
  }

  @Test
  def test_DynamoDbObjectStore_GetItem_WithTuple_ReturnsCorrectTuple(): Unit = {
    val client = new TestDynamoDbClient
    val valueType = TypeDescriptor.createTuple[(Int, IntRecord)](List(types.Int, TypeDescriptor.of[IntRecord])).toOption
    val target = DynamoDbObjectStore.create[Int, Option[(Int, IntRecord)]](client, "table", TypeDescriptor.of[Int], valueType)

    val original = Some((1, IntRecord(5)))
    target.putItem(0, original)

    val copy = target.getItem(0)
    assertEquals(Some(original), copy)
  }
}


class TestDynamoDbClient extends DynamoDbClient {
  var putItems: List[PutItemRequest] = List.empty

  override def serviceName(): String = "dynamodb"

  override def close(): Unit = ()

  override def putItem(putItemRequest: PutItemRequest): PutItemResponse = {
    this.putItems = this.putItems :+ putItemRequest
    PutItemResponse.builder().build()
  }

  override def getItem(getItemRequest: GetItemRequest): GetItemResponse = {
    GetItemResponse.builder()
      .item(putItems.head.item())
      .build()
  }
}
