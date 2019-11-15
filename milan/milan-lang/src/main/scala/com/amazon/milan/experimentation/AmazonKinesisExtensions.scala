package com.amazon.milan.experimentation

import java.time.Instant
import java.util.Date

import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model._
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}


object AmazonKinesisExtensions {
  private val logger = Logger(LoggerFactory.getLogger(getClass))

  implicit class StreamDataDownloader(val kinesis: AmazonKinesis) {
    /**
     * Downloads all records from the specified stream for a given time range and deserializes the record data into
     * objects of the specified type.
     *
     * @param streamName Name of the stream to download the data from.
     * @param startDate  Start of the time range.
     * @param endDate    End of the time range.
     * @return List of downloaded records.
     */
    def downloadStreamDataForTimeRange[T: ClassTag](streamName: String, startDate: Date, endDate: Date): List[T] = {
      val records = this.downloadStreamRecordsForTimeRange(streamName, startDate, endDate)
      val mapper = new ScalaObjectMapper()
      val valueType = classTag[T].runtimeClass.asInstanceOf[Class[T]]
      records.map(record => mapper.readValue[T](record.getData.array(), valueType))
    }

    /**
     * Downloads all records from the specified stream for a given time range.
     *
     * @param streamName Name of the stream to donwload the data from.
     * @param startDate  Start of the time range.
     * @param endDate    End of the time range.
     * @return List of downloaded records.
     */
    def downloadStreamRecordsForTimeRange(streamName: String, startDate: Date, endDate: Date): List[Record] = {
      val kinesisStream = this.kinesis.describeStream(streamName)
      val shards = kinesisStream.getStreamDescription.getShards
      var shardIds = shards.iterator.asScala.map(s => s.getShardId)

      val queryShard = (shardId: String) => {
        logger.info(s"Querying shard $shardId of stream $streamName")

        // Get shard iterator to start going through the records
        val shardIteratorRequest = new GetShardIteratorRequest().withStreamName(streamName)
          .withShardId(shardId).withShardIteratorType(ShardIteratorType.AT_TIMESTAMP)
          .withTimestamp(startDate)

        var shardIterator = kinesis.getShardIterator(shardIteratorRequest).getShardIterator

        var allShardRecords = scala.collection.mutable.ListBuffer[Record]()

        var queryAgain = true
        while (queryAgain) {
          try {
            val getRecordsRequest = new GetRecordsRequest().withShardIterator(shardIterator)
            val result = kinesis.getRecords(getRecordsRequest)
            val records = result.getRecords

            records.asScala.foreach(r =>
              if (endDate.compareTo(r.getApproximateArrivalTimestamp) > 0)
                allShardRecords += r
              else
                queryAgain = false)

            // Stop iterating if we are closer to the end of the stream than the end of the range
            val now = Date.from(Instant.now)
            val endToNowMillis = now.getTime - endDate.getTime
            if (result.getMillisBehindLatest < endToNowMillis) {
              queryAgain = false
            }

            // GetRecords call returns shard iterator to look for the next batch of records
            shardIterator = result.getNextShardIterator
            if (shardIterator == null || shardIterator.isEmpty) {
              queryAgain = false
            }
          }
          catch {
            case _: ProvisionedThroughputExceededException =>
              logger.info("Exceeded stream throughput. Sleeping for 1 second...")
              Thread.sleep(1000)
          }
        }

        logger.info(s"Finished downloading records from shard $shardId. Downloaded ${allShardRecords.size} records.")

        allShardRecords.toList
      }

      val allStreamRecords = shardIds.flatMap(queryShard).toList

      logger.info(s"Total number of records downloaded from stream $streamName: " + allStreamRecords.size)

      allStreamRecords
    }
  }

}
