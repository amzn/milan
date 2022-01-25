package com.amazon.milan.aws.serverless.runtime


case class MilanDynamoDbRecord(NewImage: Map[String, MilanDynamoDbAttributeValue])

/**
 * A record that is the union of an SQS record and a DynamoDb record.
 */
case class MilanLambdaRecord(eventSource: String,
                             eventSourceARN: String,
                             dynamodb: MilanDynamoDbRecord,
                             body: String)

case class MilanLambdaEvent(Records: List[MilanLambdaRecord])
