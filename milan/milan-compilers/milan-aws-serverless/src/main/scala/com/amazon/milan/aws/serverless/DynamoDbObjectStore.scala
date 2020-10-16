package com.amazon.milan.aws.serverless

import java.util

import com.amazon.milan.compiler.scala.event.KeyedStateInterface
import com.amazon.milan.serialization.{JavaTypeFactory, MilanObjectMapper}
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.{ObjectReader, ObjectWriter}
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, GetItemRequest, PutItemRequest}


object DynamoDbObjectStore {
  def open[TKey, TValue](tableName: String,
                         keyType: TypeDescriptor[TKey],
                         valueType: TypeDescriptor[TValue]): DynamoDbObjectStore[TKey, TValue] = {
    val typeFactory = new JavaTypeFactory(MilanObjectMapper.getTypeFactory)
    val valueJavaType = typeFactory.makeJavaType(valueType)
    val valueReader = MilanObjectMapper.readerFor(valueJavaType)
    val valueWriter = MilanObjectMapper.writerFor(valueJavaType)
    val client =
      DynamoDbClient.builder()
        .credentialsProvider(DefaultCredentialsProvider.create())
        .region(new DefaultAwsRegionProviderChain().getRegion)
        .build()
    new DynamoDbObjectStore(client, tableName, valueReader, valueWriter)
  }

  def createKeyedStateInterface[TKey, TState](tableName: String,
                                              keyType: TypeDescriptor[TKey],
                                              stateType: TypeDescriptor[TState],
                                              defaultValue: TState): KeyedStateInterface[TKey, TState] = {
    val objectStore = DynamoDbObjectStore.open[TKey, TState](tableName, keyType, stateType)
    new ObjectStoreKeyedStateInterface(objectStore, defaultValue)
  }
}


class DynamoDbObjectStore[TKey, TValue](client: DynamoDbClient,
                                        tableName: String,
                                        valueReader: ObjectReader,
                                        valueWriter: ObjectWriter)
  extends ObjectStore[TKey, TValue] {

  override def getItem(key: TKey): Option[TValue] = {
    val keyMap = new util.HashMap[String, AttributeValue]()
    keyMap.put("key", AttributeValue.builder().s(key.toString).build())

    val request =
      GetItemRequest.builder()
        .tableName(this.tableName)
        .key(keyMap)
        .build()

    val response = this.client.getItem(request)

    if (response.hasItem) {
      val jsonString = response.item().get("value").s()
      Some(this.valueReader.readValue[TValue](jsonString))
    }
    else {
      None
    }
  }

  override def putItem(key: TKey, item: TValue): Unit = {
    val valueJson = this.valueWriter.writeValueAsString(item)

    val values = new util.HashMap[String, AttributeValue]()
    values.put("key", AttributeValue.builder().s(key.toString).build())
    values.put("value", AttributeValue.builder().s(valueJson).build())

    val request =
      PutItemRequest.builder()
        .tableName(this.tableName)
        .item(values)
        .build()

    this.client.putItem(request)
  }
}
