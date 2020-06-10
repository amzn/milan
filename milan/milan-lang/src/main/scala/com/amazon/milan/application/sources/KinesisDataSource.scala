package com.amazon.milan.application.sources

import com.amazon.milan.application.DataSource
import com.amazon.milan.dataformats.{DataInputFormat, JsonDataInputFormat}
import com.amazon.milan.serialization.DataFormatConfiguration
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


object KinesisDataSource {
  /**
   * Creates a [[KinesisDataSource]] using the default JSON data format for the type.
   *
   * @param streamName The name of a Kinesis stream.
   * @param region     The region where the stream is located.
   * @tparam T The type of records on the stream.
   * @return A [[KinesisDataSource]] for the stream.
   */
  def createJson[T: TypeDescriptor](streamName: String, region: String): KinesisDataSource[T] = {
    new KinesisDataSource[T](streamName, region, new JsonDataInputFormat[T]())
  }
}


/**
 * A [[DataSource]] for reading items from a Kinesis stream.
 *
 * @param streamName The name of a Kinesis stream.
 * @param region     The region where the stream is located.
 * @param dataFormat A [[DataInputFormat]] that controls how items are read.
 * @tparam T The type of objects produced by the data source.
 */
@JsonSerialize
@JsonDeserialize
class KinesisDataSource[T: TypeDescriptor](val streamName: String,
                                           val region: String,
                                           val dataFormat: DataInputFormat[T])
  extends DataSource[T] {

  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]

  def this(streamName: String, region: String) {
    this(streamName, region, new JsonDataInputFormat[T](DataFormatConfiguration.default))
  }

  override def getGenericArguments: List[TypeDescriptor[_]] = List(this.recordTypeDescriptor)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }
}
