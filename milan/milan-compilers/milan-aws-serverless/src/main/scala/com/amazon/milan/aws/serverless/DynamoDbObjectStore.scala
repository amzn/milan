package com.amazon.milan.aws.serverless

import com.amazon.milan.compiler.scala.event.KeyedStateInterface
import com.amazon.milan.serialization.{JavaTypeFactory, MilanObjectMapper}
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.{ObjectReader, ObjectWriter}
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, GetItemRequest, PutItemRequest}

import java.util


object DynamoDbObjectStore {
  /**
   * Opens a connection to a DynamoDb table and returns an interface to it.
   *
   * @param tableName The name of the DynamoDb table.
   * @param keyType   A [[TypeDescriptor]] describing the key type.
   * @param valueType A [[TypeDescriptor]] describing the value type.
   * @return A [[DynamoDbObjectStore]] object that provides the interface to the table.
   */
  def open[TKey, TValue](tableName: String,
                         keyType: TypeDescriptor[TKey],
                         valueType: TypeDescriptor[TValue]): DynamoDbObjectStore[TKey, TValue] = {
    val client =
      DynamoDbClient.builder()
        .credentialsProvider(DefaultCredentialsProvider.create())
        .region(new DefaultAwsRegionProviderChain().getRegion)
        .build()
    create(client, tableName, keyType, valueType)
  }

  /**
   * Creates a DynamoDbObjectStore for the given key and value types.
   *
   * @param client    The dynamodb client used by the store.
   * @param tableName The name of the DynamoDb table.
   * @param keyType   A [[TypeDescriptor]] describing the key type.
   * @param valueType A [[TypeDescriptor]] describing the value type.
   * @return A [[DynamoDbObjectStore]] object that provides the interface to the table.
   */
  def create[TKey, TValue](client: DynamoDbClient,
                           tableName: String,
                           keyType: TypeDescriptor[TKey],
                           valueType: TypeDescriptor[TValue]): DynamoDbObjectStore[TKey, TValue] = {
    val typeFactory = new JavaTypeFactory(MilanObjectMapper.getTypeFactory)
    val valueTypeReference = typeFactory.makeTypeReference(valueType)
    val valueReader = MilanObjectMapper.readerFor(valueTypeReference)
    val valueWriter = MilanObjectMapper.writerFor(valueTypeReference)
    new DynamoDbObjectStore(client, tableName, valueReader, valueWriter)
  }

  /**
   * Creates a [[KeyedStateInterface]] using a DynamoDb table for state storage.
   *
   * @param tableName The name of the DynamoDb table.
   * @param keyType   A [[TypeDescriptor]] describing the key type.
   * @param stateType A [[TypeDescriptor]] describing the state type.
   * @return A [[KeyedStateInterface]] instance.
   */
  def createKeyedStateInterface[TKey, TState](tableName: String,
                                              keyType: TypeDescriptor[TKey],
                                              stateType: TypeDescriptor[TState]): KeyedStateInterface[TKey, TState] = {
    val objectStore = DynamoDbObjectStore.open[TKey, TState](tableName, keyType, stateType)
    new ObjectStoreKeyedStateInterface(objectStore)
  }
}


/**
 * An [[ObjectStore]] implementation that stores objects in a DynamoDb table.
 *
 * @param client      A DynamoDb client.
 * @param tableName   The name of the DynamoDb table.
 * @param valueReader An [[ObjectReader]] that deserializes values.
 * @param valueWriter An [[ObjectWriter]] that serializes values.
 */
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
