package com.amazon.milan.flink.runtime

import java.util.Properties

import com.amazon.milan.dataformats.DataInputFormat
import com.amazon.milan.serialization.MilanObjectMapper
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.{AWSConfigConstants, ConsumerConfigConstants}
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema
import org.slf4j.LoggerFactory


object KinesisDataSource {
  private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  def addDataSource[T](env: StreamExecutionEnvironment,
                       streamName: String,
                       region: String,
                       dataFormat: DataInputFormat[T],
                       recordTypeInformation: TypeInformation[T]): DataStreamSource[T] = {
    this.logger.info(s"Creating Kinesis consumer for stream '$streamName', region '$region'.")

    val config = this.getConsumerProperties(region)
    val schema = new JsonDeserializationSchema[T](recordTypeInformation)

    val source = new FlinkKinesisConsumer[T](streamName, schema, config)
    env.addSource(source)
  }

  private def getConsumerProperties(region: String): Properties = {
    val config = new Properties()

    config.setProperty(AWSConfigConstants.AWS_REGION, region)
    config.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, AWSConfigConstants.CredentialProvider.AUTO.toString)
    config.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, ConsumerConfigConstants.InitialPosition.LATEST.toString)

    config
  }
}


/**
 * A [[KinesisDeserializationSchema]] that handles deserializing the record data as a JSON object.
 *
 * @tparam T The type of object to deserialize from the JSON record data.
 */
class JsonDeserializationSchema[T](recordTypeInformation: TypeInformation[T])
  extends KinesisDeserializationSchema[T] {

  override def deserialize(bytes: Array[Byte],
                           partitionKey: String,
                           seqNum: String,
                           approxArrivalTimestamp: Long,
                           stream: String,
                           shardId: String): T = {
    MilanObjectMapper.readValue[T](bytes, this.recordTypeInformation.getTypeClass)
  }

  override def getProducedType: TypeInformation[T] =
    this.recordTypeInformation
}
