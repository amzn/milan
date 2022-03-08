package com.amazon.milan.aws.serverless.runtime

import com.amazon.milan.serialization.MilanObjectMapper
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.SendMessageRequest

import scala.reflect.{ClassTag, classTag}


object SqsRecordWriter {
  def open[T: ClassTag](queueUrl: String): SqsRecordWriter[T] = {
    val region = new DefaultAwsRegionProviderChain().getRegion

    val client = SqsClient.builder()
      .credentialsProvider(DefaultCredentialsProvider.create())
      .region(region)
      .build()

    new SqsRecordWriter[T](client, queueUrl)
  }
}


class SqsRecordWriter[T: ClassTag](client: SqsClient, queueUrl: String) {
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val writer = MilanObjectMapper.writerFor(classTag[T].runtimeClass)

  def writeRecord(record: T): Unit = {
    this.logger.info(s"Writing record to SQS queue '${this.queueUrl}'.")

    val body = this.writer.writeValueAsString(record)

    val request = SendMessageRequest.builder()
      .queueUrl(this.queueUrl)
      .messageBody(body)
      .build()

    client.sendMessage(request)
  }
}
