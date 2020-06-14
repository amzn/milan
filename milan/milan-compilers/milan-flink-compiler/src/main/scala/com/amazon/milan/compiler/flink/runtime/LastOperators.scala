package com.amazon.milan.compiler.flink.runtime

import com.amazon.milan.compiler.flink.types.{RecordWrapper, RecordWrapperTypeInformation}
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.state.{BroadcastState, ListState, ListStateDescriptor, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, OneInputStreamOperator, TimestampedCollector}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


/**
 * Base class for generated implementations of the last() operation on non-keyed streams.
 *
 * @param recordTypeInformation [[TypeInformation]] for the record type.
 * @param keyTypeInformation    [[TypeInformation]] for the key type.
 * @tparam T    The value type of the records.
 * @tparam TKey The key type of the records.
 */
abstract class UnkeyedLastByOperator[T >: Null, TKey >: Null <: Product](recordTypeInformation: TypeInformation[T],
                                                                         keyTypeInformation: TypeInformation[TKey])
  extends AbstractStreamOperator[RecordWrapper[T, TKey]] with OneInputStreamOperator[RecordWrapper[T, TKey], RecordWrapper[T, TKey]] {

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  // We have to store things in operator state, because we'll be collecting items when triggered by a watermark,
  // which occurs outside of the keyed context.
  @transient private var latestRecords: ListState[RecordWrapper[T, TKey]] = _

  @transient private var collector: TimestampedCollector[RecordWrapper[T, TKey]] = _

  def getProducedType: TypeInformation[RecordWrapper[T, TKey]] =
    RecordWrapperTypeInformation.wrap(this.recordTypeInformation, this.keyTypeInformation)

  /**
   * Gets whether a new value should be taken over the current value stored for a key.
   *
   * @param newRecord     The newly arrived value.
   * @param currentRecord The current value for the corresponding key.
   * @return True if the new value should replace the current value in the cache.
   */
  protected def takeNewValue(newRecord: RecordWrapper[T, TKey], currentRecord: RecordWrapper[T, TKey]): Boolean

  override def processElement(streamRecord: StreamRecord[RecordWrapper[T, TKey]]): Unit = {
    if (!this.latestRecords.get().iterator().hasNext) {
      this.latestRecords.update(List(streamRecord.getValue).asJava)
    }
    else if (this.takeNewValue(streamRecord.getValue, this.latestRecords.get().iterator().next())) {
      this.latestRecords.update(List(streamRecord.getValue).asJava)
    }
  }

  override def processWatermark(mark: Watermark): Unit = {
    if (mark.equals(Watermark.MAX_WATERMARK)) {
      this.logger.info("Got MAX_WATERMARK, collecting cached records.")
      this.latestRecords.get().asScala.foreach(this.collector.collect)
      this.latestRecords.clear()
    }

    super.processWatermark(mark)
  }

  override def open(): Unit = {
    super.open()

    this.collector = new TimestampedCollector[RecordWrapper[T, TKey]](this.output)

    val latestRecordDescriptor = new ListStateDescriptor[RecordWrapper[T, TKey]]("latestRecord", this.getProducedType)
    this.latestRecords = this.getOperatorStateBackend.getListState(latestRecordDescriptor)
  }
}


/**
 * Base class for generated implementations of the last() operation on keyed streams.
 *
 * @param recordTypeInformation [[TypeInformation]] for the record type.
 * @param keyTypeInformation    [[TypeInformation]] for the key type.
 * @tparam T    The stream's record type.
 * @tparam TKey The stream's key type.
 */
abstract class KeyedLastByOperator[T >: Null, TKey >: Null <: Product](recordTypeInformation: TypeInformation[T],
                                                                       keyTypeInformation: TypeInformation[TKey])
  extends AbstractStreamOperator[RecordWrapper[T, TKey]] with OneInputStreamOperator[RecordWrapper[T, TKey], RecordWrapper[T, TKey]] {

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  // We have to store things in operator state, because we'll be collecting items when triggered by a watermark,
  // which occurs outside of the keyed context.
  @transient private var latestRecords: BroadcastState[TKey, RecordWrapper[T, TKey]] = _

  @transient private var collector: TimestampedCollector[RecordWrapper[T, TKey]] = _

  def getProducedType: TypeInformation[RecordWrapper[T, TKey]] =
    RecordWrapperTypeInformation.wrap(this.recordTypeInformation, this.keyTypeInformation)

  /**
   * Gets whether a new value should be taken over the current value stored for a key.
   *
   * @param newRecord     The newly arrived value.
   * @param currentRecord The current value for the corresponding key.
   * @return True if the new value should replace the current value in the cache.
   */
  protected def takeNewValue(newRecord: RecordWrapper[T, TKey], currentRecord: RecordWrapper[T, TKey]): Boolean

  override def processElement(streamRecord: StreamRecord[RecordWrapper[T, TKey]]): Unit = {
    val newRecord = streamRecord.getValue
    val key = newRecord.key

    if (!this.latestRecords.contains(key) || this.takeNewValue(newRecord, this.latestRecords.get(key))) {
      this.logger.debug(s"New record for key '$key'.")
      this.latestRecords.put(key, newRecord)
    }
  }

  override def processWatermark(mark: Watermark): Unit = {
    if (mark.equals(Watermark.MAX_WATERMARK)) {
      this.logger.info("Got MAX_WATERMARK, collecting cached records.")

      this.latestRecords.entries().asScala.foreach(entry => {
        this.logger.info(s"Collecting record for key ${entry.getKey}: ${entry.getValue}")
        this.collector.collect(entry.getValue)
      })
    }

    super.processWatermark(mark)
  }

  override def open(): Unit = {
    super.open()

    this.collector = new TimestampedCollector[RecordWrapper[T, TKey]](this.output)

    val wrapperTypeInformation = RecordWrapperTypeInformation.wrap(this.recordTypeInformation, this.keyTypeInformation)
    val latestRecordDescriptor = new MapStateDescriptor[TKey, RecordWrapper[T, TKey]]("latestRecords", this.keyTypeInformation, wrapperTypeInformation)
    this.latestRecords = this.getOperatorStateBackend.getBroadcastState(latestRecordDescriptor)
  }
}
