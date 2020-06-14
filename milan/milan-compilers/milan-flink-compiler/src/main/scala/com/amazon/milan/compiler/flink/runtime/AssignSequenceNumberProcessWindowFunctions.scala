package com.amazon.milan.compiler.flink.runtime

import java.lang

import com.amazon.milan.compiler.flink.types.RecordWrapper
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector


/**
 * A [[ProcessAllWindowFunction]] that assigns increasing sequence numbers to output records.
 *
 * @tparam T       The record value type.
 * @tparam TKey    The record key type.
 * @tparam TWindow The window type.
 */
class AssignSequenceNumberProcessAllWindowFunction[T >: Null, TKey >: Null <: Product, TWindow <: Window]
  extends ProcessAllWindowFunction[RecordWrapper[T, TKey], RecordWrapper[T, TKey], TWindow] {

  @transient private var sequenceNumberHelper: SequenceNumberHelper = _

  override def process(context: ProcessAllWindowFunction[RecordWrapper[T, TKey], RecordWrapper[T, TKey], TWindow]#Context,
                       items: lang.Iterable[RecordWrapper[T, TKey]],
                       collector: Collector[RecordWrapper[T, TKey]]): Unit = {
    val item = items.iterator().next()
    val outputRecord = item.withSequenceNumber(this.sequenceNumberHelper.increment())
    collector.collect(outputRecord)
  }

  override def open(parameters: Configuration): Unit = {
    this.sequenceNumberHelper = new SequenceNumberHelper(this.getRuntimeContext)
  }
}
