package com.amazon.milan.aws.serverless.runtime

import com.amazon.milan.serialization.MilanObjectMapper
import org.junit.Assert._
import org.junit.Test


class TestMilanLambdaEvent {
  @Test
  def test_MilanLambdaEvent_DeserializeFromString_WithDynamoDbRecord(): Unit = {
    val eventString =
      """
        |{
        |    "Records": [
        |        {
        |            "eventID": "433e25c04e69c16e02e9d10ddb13b383",
        |            "eventName": "MODIFY",
        |            "eventVersion": "1.1",
        |            "eventSource": "aws:dynamodb",
        |            "awsRegion": "eu-west-1",
        |            "dynamodb": {
        |                "ApproximateCreationDateTime": 1638887556,
        |                "Keys": {
        |                    "recordId": {
        |                        "S": "testRecord"
        |                    }
        |                },
        |                "NewImage": {
        |                    "recordId": {
        |                        "S": "testRecord"
        |                    }
        |                },
        |                "SequenceNumber": "4025100000000021097195306",
        |                "SizeBytes": 85,
        |                "StreamViewType": "NEW_IMAGE"
        |            },
        |            "eventSourceARN": "arn:aws:dynamodb:eu-west-1:8675309:table/table-name/stream/2021-12-06T16:03:11.706"
        |        }
        |    ]
        |}
        |""".stripMargin

    val event = MilanObjectMapper.readValue[MilanLambdaEvent](eventString, classOf[MilanLambdaEvent])
    assertEquals("aws:dynamodb", event.Records.head.eventSource)

    val newImage = event.Records.head.dynamodb.NewImage
    assertEquals("testRecord", newImage("recordId").S)
  }

  @Test
  def test_MilanLambdaEvent_DeserializeFromString_WithSqsRecord(): Unit = {
    val eventString =
      """
        |{
        |    "Records": [
        |        {
        |            "messageId": "messageId",
        |            "receiptHandle": "receiptHandle",
        |            "body": "test message",
        |            "attributes": {
        |                "ApproximateReceiveCount": "1",
        |                "SentTimestamp": "105",
        |                "SenderId": "test:test",
        |                "ApproximateFirstReceiveTimestamp": "100"
        |            },
        |            "messageAttributes": {},
        |            "md5OfBody": "md5",
        |            "eventSource": "aws:sqs",
        |            "eventSourceARN": "arn:aws:sqs:eu-west-1:8675309:test-queue",
        |            "awsRegion": "eu-west-1"
        |        }
        |    ]
        |}
        |""".stripMargin

    val event = MilanObjectMapper.readValue[MilanLambdaEvent](eventString, classOf[MilanLambdaEvent])

    assertEquals("aws:sqs", event.Records.head.eventSource)
    assertEquals("test message", event.Records.head.body)
  }

  @Test
  def test_MilanLambdaEvent_DeserializeFromString_WithDynamoDbAndSqsRecords(): Unit = {
    val eventString =
      """
        |{
        |    "Records": [
        |        {
        |            "eventID": "eventId",
        |            "eventName": "MODIFY",
        |            "eventVersion": "1.1",
        |            "eventSource": "aws:dynamodb",
        |            "awsRegion": "eu-west-1",
        |            "dynamodb": {
        |                "ApproximateCreationDateTime": 1638887556,
        |                "Keys": {
        |                    "recordId": {
        |                        "S": "testRecord"
        |                    }
        |                },
        |                "NewImage": {
        |                    "recordId": {
        |                        "S": "testRecord"
        |                    }
        |                },
        |                "SequenceNumber": "4025100000000021097195306",
        |                "SizeBytes": 85,
        |                "StreamViewType": "NEW_IMAGE"
        |            },
        |            "eventSourceARN": "arn:aws:dynamodb:eu-west-1:8675309:table/table-name/stream/2021-12-06T16:03:11.706"
        |        },
        |        {
        |            "messageId": "messageId",
        |            "receiptHandle": "receiptHandle",
        |            "body": "test message",
        |            "attributes": {
        |                "ApproximateReceiveCount": "1",
        |                "SentTimestamp": "105",
        |                "SenderId": "test:test",
        |                "ApproximateFirstReceiveTimestamp": "100"
        |            },
        |            "messageAttributes": {},
        |            "md5OfBody": "md5",
        |            "eventSource": "aws:sqs",
        |            "eventSourceARN": "arn:aws:sqs:eu-west-1:8675309:test-queue",
        |            "awsRegion": "eu-west-1"
        |        }
        |    ]
        |}
        |""".stripMargin

    val event = MilanObjectMapper.readValue[MilanLambdaEvent](eventString, classOf[MilanLambdaEvent])

    assertEquals(2, event.Records.length)

    assertEquals("aws:dynamodb", event.Records.head.eventSource)
    val newImage = event.Records.head.dynamodb.NewImage
    assertEquals("testRecord", newImage("recordId").S)

    assertEquals("aws:sqs", event.Records.last.eventSource)
    assertEquals("aws:sqs", event.Records.last.eventSource)
    assertEquals("test message", event.Records.last.body)
  }
}
