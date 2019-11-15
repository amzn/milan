package com.amazon.milan.control

import java.text.SimpleDateFormat
import java.time.Instant
import java.util.Date

import com.amazon.milan.storage._
import com.amazonaws.regions.Regions

import scala.collection.JavaConverters._
import scala.util.matching.Regex

object SnapshotUtil {
  /** Gets the _metadata file path of the latest snapshot stored for a given date.
   * If no date is provided, then the latest snapshot ever created is returned.
   *
   * @param s3Bucket     S3 bucket where the snapshot is stored.
   * @param s3Prefix     S3 prefix of the snapshot location.
   * @param snapshotDate Creation date of the snapshot.
   *                     If provided, latest snapshot created on this date will be returned, if available. Else, None will be returned.
   *                     Else, latest snapshot ever created will be returned, if available. Else, None will be returned.
   * @return S3 Path of the _metadata file of the latest snapshot. None if requested snapshot is not available.
   */
  def getLatestSnapshotPath(region: Regions,
                            s3Bucket: String,
                            s3Prefix: String,
                            snapshotDate: Option[Date] = None): Option[String] = {
    val s3ClientFactory = new RegionS3ClientFactory(region)
    this.getLatestSnapshotPath(s3ClientFactory, s3Bucket, s3Prefix, snapshotDate)
  }

  /** Gets the _metadata file path of the latest snapshot stored for a given date.
   * If no date is provided, then the latest snapshot ever created is returned.
   *
   * @param s3ClientFactory Factory interface for creating S3 client.
   * @param s3Bucket        S3 bucket where the snapshot is stored.
   * @param s3Prefix        S3 prefix of the snapshot location.
   * @param snapshotDate    Creation date of the snapshot.
   *                        If provided, latest snapshot created on this date will be returned, if available. Else, None will be returned.
   *                        Else, latest snapshot ever created will be returned, if available. Else, None will be returned.
   * @return S3 Path of the _metadata file of the latest snapshot. None if requested snapshot is not available.
   */
  def getLatestSnapshotPath(s3ClientFactory: S3ClientFactory,
                            s3Bucket: String,
                            s3Prefix: String,
                            snapshotDate: Option[Date]): Option[String] = {
    // Create S3 client
    val s3Client = s3ClientFactory.createClient()

    // Get list of all snapshot objects stored in the given s3 location.
    val summaryList = s3Client.listObjects(s3Bucket, s3Prefix).getObjectSummaries.asScala.toList

    // Define regex pattern to extract the timestamp from snapshot paths to find the latest one.
    // If a snapshotDate is not provided, use the default pattern. Else, create a pattern with that date.
    val timestampPattern = this.getTimestampPattern(snapshotDate)

    // Iterate through the list of objects and find the latest metadata path.
    val pathsWithMatchingTimestamps = summaryList
      .map(summary => summary.getKey)
      .filter(_.contains("_metadata"))
      .map(key => (key, timestampPattern.findFirstIn(key).map(Instant.parse)))
      .filter { case (_, t) => t.isDefined }

    if (pathsWithMatchingTimestamps.nonEmpty) {
      val pathWithLatestMetadata = pathsWithMatchingTimestamps.maxBy { case (_, t) => t.get }
      Some(s"s3://$s3Bucket/${pathWithLatestMetadata._1}")
    }
    else
      None
  }

  private def getTimestampPattern(snapshotDate: Option[Date]): Regex = {
    // Define patterns for different components in a timestamp: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
    val twoDigits = s"\\d{2}"
    val threeDigits = s"\\d{3}"
    val fourDigits = s"\\d{4}"
    val date = s"$fourDigits-$twoDigits-$twoDigits" // yyyy-MM-dd
    val hourMinSec = s"$twoDigits:$twoDigits:$twoDigits" // HH:mm:ss
    val milliSeconds = s"$threeDigits" // SSS

    // If a snapshotDate is not provided, use the default pattern. Else, create a pattern with that date.
    if (snapshotDate.isEmpty)
      s"${date}T${hourMinSec}.${milliSeconds}Z".r
    else {
      val snapshotDateFormatted = new SimpleDateFormat("yyyy-MM-dd").format(snapshotDate.get)
      s"${snapshotDateFormatted}T${hourMinSec}.${milliSeconds}Z".r
    }
  }
}