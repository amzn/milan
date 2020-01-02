package com.amazon.milan.flink.compiler.internal

import java.time.{Duration, Instant}

import com.amazon.milan.program.FunctionDef
import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark


class TimeExtractorEventTimeAssigner[T](inputType: TypeDescriptor[_],
                                        timeExtractorFunc: FunctionDef,
                                        watermarkDelay: Duration)
  extends AssignerWithPunctuatedWatermarks[T] {

  private val watermarkDelayMillis = watermarkDelay.toMillis
  private val timeExtractor = new RuntimeCompiledFunction[T, Instant](this.inputType, this.timeExtractorFunc)

  override def checkAndGetNextWatermark(record: T, extractedTimestamp: Long): Watermark = {
    new Watermark(extractedTimestamp - this.watermarkDelayMillis)
  }

  override def extractTimestamp(record: T, previousElementTimestamp: Long): Long = {
    val recordTime = this.timeExtractor(record)
    recordTime.toEpochMilli
  }
}
