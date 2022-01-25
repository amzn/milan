package com.amazon.milan.aws.serverless

import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import java.util


/**
 * Interface for classes that serialize objects into DynamoDb records.
 *
 * @tparam T The type of object that can be serialized.
 */
trait DynamoDbSerializer[T] {
  def getAttributeValueMap(value: T): util.HashMap[String, AttributeValue]
}
