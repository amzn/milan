package com.amazon.milan.control.aws

import java.nio.ByteBuffer

import com.amazon.milan.control.client.StreamApplicationControllerClient
import com.amazon.milan.control.{ApplicationControllerMessageEnvelope, ApplicationControllerState, DEFAULT_CONTROLLER_ID}
import com.amazon.milan.serialization.ScalaObjectMapper
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest


object ControllerClient {
  private val logger = Logger(LoggerFactory.getLogger(getClass))

  /**
   * Creates a [[StreamApplicationControllerClient]] that uses Kinesis streams.
   *
   * @param messageStreamName The name of the Kinesis stream to send controller messages to.
   * @param stateStreamName   The name of the Kinesis stream from which controller state can be read.
   * @param controllerId      The application controller ID (usually 'default').
   * @return A [[StreamApplicationControllerClient]] instance.
   */
  def createForKinesisStreams(messageStreamName: String,
                              stateStreamName: String,
                              controllerId: String = DEFAULT_CONTROLLER_ID,
                              region: Region = Region.EU_WEST_1): StreamApplicationControllerClient = {
    val kinesisClient = KinesisClient.builder()
      .credentialsProvider(DefaultCredentialsProvider.create())
      .region(region)
      .build()

    val objectMapper = new ScalaObjectMapper()

    def messageSink(envelope: ApplicationControllerMessageEnvelope): Unit = {
      this.logger.info(s"Adding controller message to stream '$messageStreamName'.")
      val envelopeBytes = objectMapper.writeValueAsBytes(envelope)
      val buffer = ByteBuffer.wrap(envelopeBytes)
      kinesisClient.putRecord(PutRecordRequest.builder().streamName(messageStreamName).data(SdkBytes.fromByteBuffer(buffer)).partitionKey("0").build())
    }

    val stateReader = KinesisIteratorObjectReader.forStreamWithOneShard[ApplicationControllerState](kinesisClient, stateStreamName)

    new StreamApplicationControllerClient(messageSink, () => stateReader.next(), controllerId)
  }

}
