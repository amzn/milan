package com.amazon.milan.compiler.flink.runtime

import java.time.{Duration, Instant}

import com.amazon.milan.compiler.flink.types.RecordWrapper
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark


abstract class InstantExtractorEventTimeAssigner[T >: Null, TKey >: Null <: Product](watermarkDelay: Duration)
  extends AssignerWithPunctuatedWatermarks[RecordWrapper[T, TKey]] {

  private val watermarkDelayMillis = watermarkDelay.toMillis

  protected def getInstant(value: T): Instant

  override def checkAndGetNextWatermark(record: RecordWrapper[T, TKey], extractedTimestamp: Long): Watermark = {
    new Watermark(extractedTimestamp - this.watermarkDelayMillis)
  }

  override def extractTimestamp(record: RecordWrapper[T, TKey], previousElementTimestamp: Long): Long = {
    val recordTime = this.getInstant(record.value)
    recordTime.toEpochMilli
  }
}
