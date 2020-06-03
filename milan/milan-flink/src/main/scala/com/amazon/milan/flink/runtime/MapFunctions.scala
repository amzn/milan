package com.amazon.milan.flink.runtime

import com.amazon.milan.flink.internal.LineageRecordFactory
import com.amazon.milan.flink.metrics.MetricFactory
import com.amazon.milan.flink.types.{RecordWrapper, RecordWrapperTypeInformation}
import com.amazon.milan.types.LineageRecord
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.util.OutputTag


object MapFunctions {
  val ProcessedRecordsCounterMetricName = "processed_record_count"
}

import com.amazon.milan.flink.runtime.MapFunctions._


abstract class MapFunctionWithLineage[TIn >: Null, TKey >: Null <: Product, TOut >: Null](outputTypeInformation: TypeInformation[TOut],
                                                                                          keyTypeInformation: TypeInformation[TKey],
                                                                                          lineageRecordFactory: LineageRecordFactory,
                                                                                          lineageOutputTag: OutputTag[LineageRecord],
                                                                                          metricFactory: MetricFactory)
  extends RichMapFunction[RecordWrapper[TIn, TKey], RecordWrapper[TOut, TKey]]
    with ResultTypeQueryable[RecordWrapper[TOut, TKey]] {

  @transient private lazy val processedRecordsCounter = this.metricFactory.createCounter(this.getRuntimeContext, ProcessedRecordsCounterMetricName)

  protected def mapValue(in: TIn): TOut

  override def map(record: RecordWrapper[TIn, TKey]): RecordWrapper[TOut, TKey] = {
    this.processedRecordsCounter.increment()
    val mappedValue = this.mapValue(record.value)
    RecordWrapper.wrap(mappedValue, record.key, record.sequenceNumber)
  }

  override def getProducedType: TypeInformation[RecordWrapper[TOut, TKey]] =
    RecordWrapperTypeInformation.wrap(this.outputTypeInformation, this.keyTypeInformation)
}


abstract class KeyedMapFunctionWithLineage[TIn >: Null, TInKey >: Null <: Product, TKey, TOut >: Null](outputTypeInfo: TypeInformation[TOut],
                                                                                                       keyTypeInfo: TypeInformation[TInKey],
                                                                                                       lineageRecordFactory: LineageRecordFactory,
                                                                                                       lineageOutputTag: OutputTag[LineageRecord],
                                                                                                       metricFactory: MetricFactory)
  extends RichMapFunction[RecordWrapper[TIn, TInKey], RecordWrapper[TOut, TInKey]]
    with ResultTypeQueryable[RecordWrapper[TOut, TInKey]] {

  @transient private lazy val processedRecordsCounter = this.metricFactory.createCounter(this.getRuntimeContext, ProcessedRecordsCounterMetricName)

  /**
   * Gets the grouping key from the input record key.
   */
  protected def getKey(recordKey: TInKey): TKey

  protected def mapValue(key: TKey, value: TIn): TOut

  override def map(record: RecordWrapper[TIn, TInKey]): RecordWrapper[TOut, TInKey] = {
    this.processedRecordsCounter.increment()
    val key = this.getKey(record.key)
    val mappedValue = this.mapValue(key, record.value)
    RecordWrapper.wrap(mappedValue, record.key, record.sequenceNumber)
  }

  override def getProducedType: TypeInformation[RecordWrapper[TOut, TInKey]] =
    RecordWrapperTypeInformation.wrap(this.outputTypeInfo, this.keyTypeInfo)
}
