package com.amazon.milan.control.client

import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}

import scala.reflect.{ClassTag, classTag}


object KinesisIteratorObjectReader {
  /**
   * Creates a [[KinesisIteratorObjectReader]]`[`T`]` for a Kinesis shard.
   *
   * @param streamName The name of the Kinesis stream.
   * @param shardId    The ID of the shard.
   * @tparam T The type of objects in the records.
   * @return A [[KinesisIteratorObjectReader]] that yields objects from the records.
   */
  def forShard[T: ClassTag](streamName: String,
                            shardId: String,
                            region: Regions = Regions.EU_WEST_1): KinesisIteratorObjectReader[T] = {
    val client = AmazonKinesisClientBuilder.standard()
      .withCredentials(new DefaultAWSCredentialsProviderChain())
      .withRegion(region)
      .build()

    val kinesisIterator = new KinesisLatestRecordIterator(client, streamName, shardId)
    new KinesisIteratorObjectReader[T](kinesisIterator)
  }

  /**
   * Creates a [[KinesisIteratorObjectReader]]`[`T`]` for a Kinesis stream that has only a single shard.
   * Throws an [[UnsupportedOperationException]] if the stream has more than one shard.
   *
   * @param streamName The name of the Kinesis stream.
   * @tparam T The type of objects in the records.
   * @return A [[KinesisIteratorObjectReader]] that yields objects from the records.
   */
  def forOnlyShard[T: ClassTag](streamName: String, region: Regions = Regions.EU_WEST_1): KinesisIteratorObjectReader[T] = {
    val client = AmazonKinesisClientBuilder.standard()
      .withCredentials(new DefaultAWSCredentialsProviderChain())
      .withRegion(region)
      .build()
    this.forStreamWithOneShard[T](client, streamName)
  }

  /**
   * Creates a [[KinesisIteratorObjectReader]]`[`T`]` for a Kinesis stream that has only a single shard.
   * Throws an [[UnsupportedOperationException]] if the stream has more than one shard.
   *
   * @param client     A Kinesis client.
   * @param streamName The name of the Kinesis stream.
   * @tparam T The type of objects in the records.
   * @return A [[KinesisIteratorObjectReader]] that yields objects from the records.
   */
  def forStreamWithOneShard[T: ClassTag](client: AmazonKinesis, streamName: String): KinesisIteratorObjectReader[T] = {
    val kinesisIterator = KinesisLatestRecordIterator.forStreamWithOneShard(client, streamName)
    new KinesisIteratorObjectReader[T](kinesisIterator)
  }
}


/**
 * An [[Iterator]] that wraps another [[Iterator]] of Kinesis [[Record]] object and deserializes those objects.
 *
 * @param recordIterator An [[Iterator]] that yields Kinesis records.
 * @tparam T The type of objects to deserialize from the records yielded by the record iterator.
 */
class KinesisIteratorObjectReader[+T: ClassTag](recordIterator: Iterator[Option[Record]]) extends Iterator[Option[T]] {
  private val mapper = new ScalaObjectMapper()

  override def next(): Option[T] = {
    if (!this.hasNext) {
      null
    }
    else {
      val nextRecord = this.recordIterator.next()

      if (nextRecord == null) {
        // If the inner iterator runs out then do the same here.
        null
      }
      else {
        nextRecord.map(record => {
          val data = record.getData.array()
          mapper.readValue[T](data, classTag[T].runtimeClass.asInstanceOf[Class[T]])
        })
      }
    }
  }

  override def hasNext: Boolean = this.recordIterator.hasNext

  override def hasDefiniteSize: Boolean = this.recordIterator.hasDefiniteSize
}
