package com.amazon.milan.aws.serverless.runtime

import com.amazon.milan.aws.serverless.DynamoDbSerializer
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest

object DynamoDbRecordWriter {
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  def open[T](tableName: String, recordSerializer: DynamoDbSerializer[T]): DynamoDbRecordWriter[T] = {
    val region = new DefaultAwsRegionProviderChain().getRegion

    this.logger.info(s"Opening DynamoDb table '$tableName' in region ${region.id()}")

    val client =
      DynamoDbClient.builder()
        .credentialsProvider(DefaultCredentialsProvider.create())
        .region(new DefaultAwsRegionProviderChain().getRegion)
        .build()

    new DynamoDbRecordWriter[T](client, tableName, recordSerializer)
  }
}


class DynamoDbRecordWriter[T](client: DynamoDbClient, tableName: String, recordSerializer: DynamoDbSerializer[T]) {
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  def writeRecord(record: T): Unit = {
    this.logger.info(s"Writing record to DynamoDb table '$tableName'")

    val item = this.recordSerializer.getAttributeValueMap(record)

    val request =
      PutItemRequest.builder()
        .tableName(this.tableName)
        .item(item)
        .build()

    this.client.putItem(request)
  }
}
