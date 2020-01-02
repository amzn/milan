package com.amazon.milan.experimentation

import java.time.{Duration, Instant}

import com.amazon.milan.serialization.ScalaObjectMapper
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model._

import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}


object AmazonKinesisExtensions {
  private val logger = Logger(LoggerFactory.getLogger(getClass))

  implicit class StreamDataDownloader(val kinesis: KinesisClient) {
    /**
     * Downloads all records from the specified stream for a given time range and deserializes the record data into
     * objects of the specified type.
     *
     * @param streamName Name of the stream to download the data from.
     * @param startDate  Start of the time range.
     * @param endDate    End of the time range.
     * @return List of downloaded records.
     */
    def downloadStreamDataForTimeRange[T: ClassTag](streamName: String, startDate: Instant, endDate: Instant): List[T] = {
      val records = this.downloadStreamRecordsForTimeRange(streamName, startDate, endDate)
      val mapper = new ScalaObjectMapper()
      val valueType = classTag[T].runtimeClass.asInstanceOf[Class[T]]
      records.map(record => mapper.readValue[T](record.data().asByteArray(), valueType))
    }

    /**
     * Downloads all records from the specified stream for a given time range.
     *
     * @param streamName Name of the stream to donwload the data from.
     * @param startDate  Start of the time range.
     * @param endDate    End of the time range.
     * @return List of downloaded records.
     */
    def downloadStreamRecordsForTimeRange(streamName: String, startDate: Instant, endDate: Instant): List[Record] = {
      val kinesisStream = this.kinesis.describeStream(DescribeStreamRequest.builder().streamName(streamName).build())
      val shards = kinesisStream.streamDescription().shards()
      val shardIds = shards.iterator.asScala.map(s => s.shardId())

      val queryShard = (shardId: String) => {
        logger.info(s"Querying shard $shardId of stream $streamName")

        // Get shard iterator to start going through the records
        val shardIteratorRequest = GetShardIteratorRequest.builder()
          .streamName(streamName)
          .shardId(shardId)
          .shardIteratorType(ShardIteratorType.AT_TIMESTAMP)
          .timestamp(startDate)
          .build()

        var shardIterator = kinesis.getShardIterator(shardIteratorRequest).shardIterator()

        var allShardRecords = scala.collection.mutable.ListBuffer[Record]()

        var queryAgain = true
        while (queryAgain) {
          try {
            val getRecordsRequest = GetRecordsRequest.builder().shardIterator(shardIterator).build()
            val result = kinesis.getRecords(getRecordsRequest)
            val records = result.records()

            records.asScala.foreach(r =>
              if (endDate.compareTo(r.approximateArrivalTimestamp()) > 0)
                allShardRecords += r
              else
                queryAgain = false)

            // Stop iterating if we are closer to the end of the stream than the end of the range.
            val endToNowMillis = Duration.between(endDate, Instant.now()).toMillis
            if (result.millisBehindLatest() < endToNowMillis) {
              queryAgain = false
            }

            // GetRecords call returns shard iterator to look for the next batch of records.
            shardIterator = result.nextShardIterator()
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
