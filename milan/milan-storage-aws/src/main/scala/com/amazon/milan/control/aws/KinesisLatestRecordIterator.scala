package com.amazon.milan.control.aws

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model._


object KinesisLatestRecordIterator {
  /**
   * Creates a new [[KinesisLatestRecordIterator]] for the only shard of a stream.
   * Throws an [[UnsupportedOperationException]] if the stream has more than one shard.
   *
   * @param client     A Kinesis client.
   * @param streamName The name of the stream.
   * @return A [[KinesisLatestRecordIterator]] for the shard.
   */
  def forStreamWithOneShard(client: KinesisClient, streamName: String): KinesisLatestRecordIterator = {
    val listShardsRequest = ListShardsRequest.builder().streamName(streamName).build()
    val listShardsResult = client.listShards(listShardsRequest)

    if (listShardsResult.shards().size() > 1) {
      throw new UnsupportedOperationException("Stream has more than one shard.")
    }

    val shardId = listShardsResult.shards().get(0).shardId()

    new KinesisLatestRecordIterator(client, streamName, shardId)
  }
}


/**
 * An [[Iterator]]`[`[[Record]]`]` that returns the latest record in a Kinesis shard, or null if no new records are available.
 *
 * @param client     A Kinesis client.
 * @param streamName The name of the Kinesis stream.
 * @param shardId    The shard ID.
 */
class KinesisLatestRecordIterator(client: KinesisClient, streamName: String, shardId: String)
  extends Iterator[Option[Record]] {

  private var shardIterator: String = _

  private val logger = Logger(LoggerFactory.getLogger(getClass))

  private var running: Boolean = false

  private var nextRecord: Option[Record] = None

  private var workerThread: Thread = this.startConsumer()

  /**
   * Gets the latest record from the stream, or None if no records have arrived since the last time this method
   * was called.
   *
   * @return The latest record after the previous record, or None.
   */
  override def next(): Option[Record] = {
    val recordToReturn = this.nextRecord
    this.nextRecord = None
    recordToReturn
  }

  // We always have a next record, because even if no new records are available we still return None.
  override def hasNext: Boolean = true

  override def hasDefiniteSize: Boolean = false

  override def finalize(): Unit = {
    this.stopConsumer()
    super.finalize()
  }

  private def seekNextRecord(): Option[Record] = {
    if (this.shardIterator == null) {
      // We would prefer to have a way to seek to latest record in the shard, but Kinesis doesn't allow that
      // so the best we can do is iterate over every record in the shard until we get to the most recent.
      this.logger.debug(s"Getting shard iterator from stream '${this.streamName}' at TRIM_HORIZON.")

      val getIteratorRequest = GetShardIteratorRequest.builder()
        .streamName(this.streamName)
        .shardId(this.shardId)
        .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
        .build()
      val getIteratorResult = this.client.getShardIterator(getIteratorRequest)
      this.shardIterator = getIteratorResult.shardIterator()
    }

    this.logger.debug(s"Getting next record for stream '${this.streamName}' shard '${this.shardId}'.")

    val getNextRequest = GetRecordsRequest.builder().shardIterator(this.shardIterator).limit(10).build()
    val getNextResult = this.client.getRecords(getNextRequest)

    this.shardIterator = getNextResult.nextShardIterator()

    val records = getNextResult.records()

    if (records.size() == 0) {
      if (getNextResult.millisBehindLatest() == 0) {
        // There are no records after the current timestamp, so return null.
        this.logger.debug(s"No new records for stream '${this.streamName}' shard '${this.shardId}'.")
        None
      }
      else {
        // There are records after the current position but we don't know what the current position is.
        // Keep polling until we get a record back.
        this.logger.debug(s"No record returned for stream '${this.streamName}' shard '${this.shardId}', but iterator is not current.")
        this.seekNextRecord()
      }
    }
    else {
      val thisRecord = records.get(records.size() - 1)

      if (getNextResult.millisBehindLatest() == 0) {
        // This is the latest record, so return it.
        Some(thisRecord)
      }
      else {
        // This might not be the latest record, so keep looking.
        this.seekNextRecord() match {
          case Some(record) =>
            Some(record)

          case None =>
            Some(thisRecord)
        }
      }
    }
  }

  private def startConsumer(): Thread = {
    this.running = true

    val runnable = new Runnable {
      override def run(): Unit = runConsumer()
    }

    val workerThread = new Thread(runnable)
    workerThread.start()

    workerThread
  }

  private def stopConsumer(): Unit = {
    this.running = false
    this.workerThread = null
  }

  private def runConsumer(): Unit = {
    while (this.running) {
      this.seekNextRecord() match {
        case Some(record) =>
          this.logger.info(s"Found a new latest record for stream '${this.streamName}', shard '${this.shardId}'.")
          this.nextRecord = Some(record)

        case None =>
          ()
      }

      Thread.sleep(100)
    }
  }
}
