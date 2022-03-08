package com.amazon.milan.aws.serverless.runtime

import com.amazon.milan.compiler.scala.{CodeBlock, toValidName}
import com.amazon.milan.serialization.MilanObjectMapper
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.io.{InputStream, OutputStream}

object MilanLambdaHandler {
  val handleDynamoDbNewImageMethodName: CodeBlock = CodeBlock("handleDynamoDbNewImage")

  val handleSqsBodyMethodName: CodeBlock = CodeBlock("handleSqsBody")

  def getEventSourceArnPrefixEnvironmentVariable(inputName: String): String = {
    toValidName(s"EventSourceArnPrefix_$inputName")
  }
}


abstract class MilanLambdaHandler(environment: EnvironmentAccessor) extends RequestStreamHandler {
  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  protected def handleDynamoDbNewImage(eventSourceArn: String, newImageJson: String): Unit

  protected def handleSqsBody(eventSourceArn: String, body: String): Unit

  override def handleRequest(inputStream: InputStream, outputStream: OutputStream, context: Context): Unit = {
    this.logger.info(s"Handling request ${context.getAwsRequestId}")

    val event = MilanObjectMapper.readValue[MilanLambdaEvent](inputStream, classOf[MilanLambdaEvent])

    event.Records.foreach(this.handleRecord(context))
  }

  private def handleRecord(context: Context)(record: MilanLambdaRecord): Unit = {
    this.logger.info(s"Handling record from ${record.eventSource} with event source ARN '${record.eventSourceARN}'")

    if (record.eventSource == "aws:sqs") {
      this.handleSqsBody(record.eventSourceARN, record.body)
    }
    else if (record.eventSource == "aws:dynamodb") {
      this.handleDynamodbRecord(record.eventSourceARN, record.dynamodb)
    }
    else {
      context.getLogger.log(s"Unrecognized event source ${record.eventSource} with ARN ${record.eventSourceARN}")
    }
  }

  private def handleDynamodbRecord(eventSourceArn: String, record: MilanDynamoDbRecord): Unit = {
    this.logger.info(s"Handling DynamoDb record with eventSourceArn '$eventSourceArn'")

    if (record.NewImage != null) {
      val newImageJson = DynamoDbImage.toJson(record.NewImage)
      this.logger.debug(s"NewImage:\n$newImageJson")
      this.handleDynamoDbNewImage(eventSourceArn, newImageJson)
    }
  }

  protected def getEventSourceArnPrefix(inputName: String): String = {
    this.environment.getEnv(MilanLambdaHandler.getEventSourceArnPrefixEnvironmentVariable(inputName))
  }
}
