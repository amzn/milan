package com.amazon.milan.flink.application.sinks

import java.net.InetAddress
import java.util.Properties

import com.amazon.milan.flink.application.FlinkDataSink
import com.amazon.milan.flink.serialization.JsonSerializationSchema
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants
import org.slf4j.LoggerFactory


@JsonDeserialize
class FlinkKinesisDataSink[T](val streamName: String,
                              val region: String,
                              val queueLimit: Option[Int] = None)
  extends FlinkDataSink[T] {

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(this.getClass))

  /**
   * Gets a copy of this [[FlinkKinesisDataSink]] with the specified queue limit applied.
   *
   * @param queueLimit The Kinesis queue limit value to apply.
   * @return A copy of this data sink with the specified queue limit applied.
   */
  def withQueueLimit(queueLimit: Int): FlinkKinesisDataSink[T] = {
    new FlinkKinesisDataSink[T](streamName, region, Some(queueLimit))
  }

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
  }

  override def getSinkFunction: SinkFunction[_] = {
    this.logger.info(s"Creating Kinesis producer for stream '${this.streamName}'.")
    val config = this.getProducerProperties

    val schema = new JsonSerializationSchema[T]()

    val producer = new FlinkKinesisProducer[T](schema, config)
    producer.setDefaultStream(this.streamName)
    producer.setFailOnError(false) // Errors will be logged and the producer will continue.
    producer.setDefaultPartition(InetAddress.getLocalHost.getHostAddress) // Partition based on host IP address

    this.queueLimit match {
      case Some(limit) => producer.setQueueLimit(limit)
      case None =>
    }

    producer
  }

  /**
   * Get properties for the [[FlinkKinesisProducer]].
   *
   * @return Configuration [[Properties]].
   */
  private def getProducerProperties: Properties = {
    val config = new Properties()
    config.setProperty(AWSConfigConstants.AWS_REGION, this.region)
    config.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, AWSConfigConstants.CredentialProvider.AUTO.toString)

    config
  }
}
