package com.amazon.milan.flink.application.sources

import java.util.Properties

import com.amazon.milan.dataformats.DataFormat
import com.amazon.milan.flink.RuntimeEvaluator
import com.amazon.milan.flink.application.FlinkDataSource
import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.{AWSConfigConstants, ConsumerConfigConstants}
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema
import org.slf4j.LoggerFactory


@JsonDeserialize
class FlinkKinesisDataSource[T](val streamName: String,
                                val region: String,
                                val dataFormat: DataFormat[T],
                                var recordTypeInformation: TypeInformation[T])
  extends FlinkDataSource[T] {

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  @JsonCreator
  def this(streamName: String,
           region: String,
           dataFormat: DataFormat[T]) {
    this(streamName, region, dataFormat, null)
  }

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeInformation = RuntimeEvaluator.instance.createTypeInformation[T](genericArgs.head.asInstanceOf[TypeDescriptor[T]])
  }

  override def addDataSource(env: StreamExecutionEnvironment): DataStreamSource[T] = {
    this.logger.info(s"Creating Kinesis consumer for stream '${this.streamName}', region '${this.region}'.")

    val config = this.getConsumerProperties
    val schema = new JsonDeserializationSchema[T](this.recordTypeInformation)

    val source = new FlinkKinesisConsumer[T](streamName, schema, config)
    env.addSource(source)
  }

  private def getConsumerProperties: Properties = {
    val config = new Properties()

    config.setProperty(AWSConfigConstants.AWS_REGION, this.region)
    config.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, AWSConfigConstants.CredentialProvider.AUTO.toString)
    config.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, ConsumerConfigConstants.InitialPosition.LATEST.toString)

    config
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: FlinkKinesisDataSource[T] =>
      this.streamName == o.streamName &&
        this.region == o.region &&
        this.dataFormat == o.dataFormat &&
        this.recordTypeInformation == o.recordTypeInformation

    case _ =>
      false
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
    ScalaObjectMapper.readValue[T](bytes, this.recordTypeInformation.getTypeClass)
  }

  override def getProducedType: TypeInformation[T] =
    this.recordTypeInformation
}
