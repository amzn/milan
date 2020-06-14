package com.amazon.milan.compiler.flink.runtime

import java.lang
import java.time.Instant

import com.amazon.milan.compiler.flink.TypeUtil
import com.amazon.milan.compiler.flink.types.{RecordWrapper, RecordWrapperTypeInformation}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


abstract class TimeWindowFlatMapProcessWindowFunction[T >: Null, TInKey >: Null <: Product, TOutKey >: Null <: Product](recordTypeInfo: TypeInformation[T],
                                                                                                                        outKeyTypeInfo: TypeInformation[TOutKey])
  extends ProcessWindowFunction[RecordWrapper[Option[T], TInKey], RecordWrapper[Option[T], TOutKey], TInKey, TimeWindow]
    with ResultTypeQueryable[RecordWrapper[Option[T], TOutKey]] {

  @transient private var sequenceNumberHelper: SequenceNumberHelper = _

  protected def addWindowStartTimeToKey(key: TInKey, windowStart: Instant): TOutKey

  override def getProducedType: TypeInformation[RecordWrapper[Option[T], TOutKey]] =
    RecordWrapperTypeInformation.wrap(TypeUtil.createOptionTypeInfo(this.recordTypeInfo), this.outKeyTypeInfo)

  override def process(key: TInKey,
                       context: ProcessWindowFunction[RecordWrapper[Option[T], TInKey], RecordWrapper[Option[T], TOutKey], TInKey, TimeWindow]#Context,
                       items: lang.Iterable[RecordWrapper[Option[T], TInKey]],
                       collector: Collector[RecordWrapper[Option[T], TOutKey]]): Unit = {
    val windowStartTime = Instant.ofEpochMilli(context.window().getStart)

    val record = items.iterator().next()
    val outKey = this.addWindowStartTimeToKey(record.key, windowStartTime)
    val outRecord = RecordWrapper.wrap(record.value, outKey, sequenceNumberHelper.increment())
    collector.collect(outRecord)
  }

  override def open(parameters: Configuration): Unit = {
    this.sequenceNumberHelper = new SequenceNumberHelper(this.getRuntimeContext)
  }
}
