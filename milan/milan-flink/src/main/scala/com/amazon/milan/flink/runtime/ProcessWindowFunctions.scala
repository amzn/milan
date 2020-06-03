package com.amazon.milan.flink.runtime

import java.lang
import java.time.Instant

import com.amazon.milan.flink.types.{AggregatorOutputRecord, RecordWrapper}
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory


trait ProcessWindowFunctionBase[TIn >: Null, TGroupKey, TOut >: Null] {

  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(this.getClass))
  @transient private var collectedInputs: MapState[String, java.lang.Boolean] = _
  @transient private var sequenceNumberHelper: SequenceNumberHelper = _

  def getOutput(key: TGroupKey, accumulator: TIn): TOut

  def process(key: TGroupKey,
              inputs: lang.Iterable[AggregatorOutputRecord[TIn]],
              collector: Collector[RecordWrapper[TOut, Product]]): Unit = {
    val newInputs = List(inputs.iterator().next())

    if (newInputs.nonEmpty) {
      this.logger.debug(s"${newInputs.length} new inputs to collect for key $key: ${newInputs.map(_.outputId).mkString(", ")}")

      newInputs.foreach(value => {
        val output = this.getOutput(key, value.value)
        collector.collect(RecordWrapper.wrap[TOut](output, this.sequenceNumberHelper.increment()))
        this.collectedInputs.put(value.outputId, true)
      })
    }
  }

  def open(runtimeContext: RuntimeContext): Unit = {
    val collectedInputsDescriptor = new MapStateDescriptor[String, java.lang.Boolean]("collectedInputs", Types.STRING, Types.BOOLEAN)
    this.collectedInputs = runtimeContext.getMapState(collectedInputsDescriptor)

    this.sequenceNumberHelper = new SequenceNumberHelper(runtimeContext)
  }
}


abstract class TimeWindowGroupByProcessWindowFunction[TIn >: Null, TKey, TOut >: Null]
  extends ProcessWindowFunction[AggregatorOutputRecord[TIn], RecordWrapper[TOut, Product], TKey, TimeWindow]
    with ProcessWindowFunctionBase[TIn, Instant, TOut] {

  override def process(key: TKey,
                       context: ProcessWindowFunction[AggregatorOutputRecord[TIn], RecordWrapper[TOut, Product], TKey, TimeWindow]#Context,
                       inputs: lang.Iterable[AggregatorOutputRecord[TIn]],
                       collector: Collector[RecordWrapper[TOut, Product]]): Unit = {
    val windowStartTime = Instant.ofEpochMilli(context.window().getStart)
    this.process(windowStartTime, inputs, collector)
  }

  override def open(parameters: Configuration): Unit = {
    this.open(this.getRuntimeContext)
  }
}


abstract class GroupByProcessWindowFunction[TIn >: Null, TInKey, TKey, TOut >: Null]
  extends ProcessWindowFunction[AggregatorOutputRecord[TIn], RecordWrapper[TOut, Product], TInKey, GlobalWindow]
    with ProcessWindowFunctionBase[TIn, TKey, TOut] {

  /**
   * Gets the grouping key from the input record key.
   */
  protected def getKey(recordKey: TInKey): TKey

  override def process(recordKey: TInKey,
                       context: ProcessWindowFunction[AggregatorOutputRecord[TIn], RecordWrapper[TOut, Product], TInKey, GlobalWindow]#Context,
                       inputs: lang.Iterable[AggregatorOutputRecord[TIn]],
                       collector: Collector[RecordWrapper[TOut, Product]]): Unit = {
    val key = this.getKey(recordKey)
    this.process(key, inputs, collector)
  }

  override def open(parameters: Configuration): Unit = {
    this.open(this.getRuntimeContext)
  }
}


abstract class TimeWindowProcessAllWindowFunction[TIn >: Null, TOut >: Null]
  extends ProcessAllWindowFunction[AggregatorOutputRecord[TIn], RecordWrapper[TOut, Product], TimeWindow]
    with ProcessWindowFunctionBase[TIn, Instant, TOut] {

  override def process(context: ProcessAllWindowFunction[AggregatorOutputRecord[TIn], RecordWrapper[TOut, Product], TimeWindow]#Context,
                       inputs: lang.Iterable[AggregatorOutputRecord[TIn]],
                       collector: Collector[RecordWrapper[TOut, Product]]): Unit = {
    val windowStartTime = Instant.ofEpochMilli(context.window().getStart)
    this.process(windowStartTime, inputs, collector)
  }

  override def open(parameters: Configuration): Unit = {
    this.open(this.getRuntimeContext)
  }
}
