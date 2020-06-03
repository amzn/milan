package com.amazon.milan.flink.runtime

import com.amazon.milan.flink.internal.JoinLineageRecordFactory
import com.amazon.milan.flink.metrics.MetricFactory
import com.amazon.milan.flink.types.{RecordWrapper, RecordWrapperTypeInformation}
import com.amazon.milan.types.LineageRecord
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, KeyedCoProcessFunction}
import org.apache.flink.util.{Collector, OutputTag}
import org.slf4j.LoggerFactory


/**
 * Flink [[CoProcessFunction]] base class for full joins.
 *
 * @param leftTypeInformation   [[TypeInformation]] for the left input stream.
 * @param rightTypeInformation  [[TypeInformation]] for the right input stream.
 * @param outputTypeInformation [[TypeInformation]] for the output stream.
 * @tparam TLeft  The left input stream record type.
 * @tparam TRight The right input stream record type.
 * @tparam TKey   The key type.
 * @tparam TOut   The output record type.
 */
abstract class FullJoinKeyedCoProcessFunction[TLeft >: Null, TRight >: Null, TKey >: Null <: Product, TOut >: Null](leftTypeInformation: TypeInformation[TLeft],
                                                                                                                    rightTypeInformation: TypeInformation[TRight],
                                                                                                                    keyTypeInformation: TypeInformation[TKey],
                                                                                                                    outputTypeInformation: TypeInformation[TOut],
                                                                                                                    leftRecordIdExtractor: RecordIdExtractor[TLeft],
                                                                                                                    rightRecordIdExtractor: RecordIdExtractor[TRight],
                                                                                                                    outputRecordIdExtractor: RecordIdExtractor[TOut],
                                                                                                                    lineageRecordFactory: JoinLineageRecordFactory,
                                                                                                                    lineageOutputTag: OutputTag[LineageRecord],
                                                                                                                    metricFactory: MetricFactory)
  extends KeyedCoProcessFunction[TKey, RecordWrapper[TLeft, TKey], RecordWrapper[TRight, TKey], RecordWrapper[TOut, TKey]]
    with ResultTypeQueryable[RecordWrapper[TOut, TKey]] {

  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  @transient private var lastRightValue: ValueState[TRight] = _
  @transient private var lastLeftValue: ValueState[TLeft] = _

  protected def postCondition(left: TLeft, right: TRight): Boolean = true

  protected def map(left: TLeft, right: TRight): TOut

  override def processElement1(leftRecord: RecordWrapper[TLeft, TKey],
                               context: KeyedCoProcessFunction[TKey, RecordWrapper[TLeft, TKey], RecordWrapper[TRight, TKey], RecordWrapper[TOut, TKey]]#Context,
                               collector: Collector[RecordWrapper[TOut, TKey]]): Unit = {
    val leftValue = leftRecord.value

    this.lastLeftValue.update(leftValue)
    val rightValue = this.lastRightValue.value()

    if (this.postCondition(leftValue, rightValue)) {
      val output = this.map(leftValue, rightValue)
      if (output != null) {
        collector.collect(RecordWrapper.wrap[TOut, TKey](output, leftRecord.key, 0))
      }
    }
  }

  override def processElement2(rightRecord: RecordWrapper[TRight, TKey],
                               context: KeyedCoProcessFunction[TKey, RecordWrapper[TLeft, TKey], RecordWrapper[TRight, TKey], RecordWrapper[TOut, TKey]]#Context,
                               collector: Collector[RecordWrapper[TOut, TKey]]): Unit = {
    val rightValue = rightRecord.value

    this.lastRightValue.update(rightValue)
    val leftValue = this.lastLeftValue.value()

    if (this.postCondition(leftValue, rightValue)) {
      val output = this.map(leftValue, rightValue)
      if (output != null) {
        collector.collect(RecordWrapper.wrap[TOut, TKey](output, rightRecord.key, 0))
      }
    }
  }

  override def open(parameters: Configuration): Unit = {
    val leftValueDescriptor = new ValueStateDescriptor[TLeft]("lastLeftValue", this.leftTypeInformation)
    this.lastLeftValue = this.getRuntimeContext.getState(leftValueDescriptor)

    val rightValueDescriptor = new ValueStateDescriptor[TRight]("lastRightValue", this.rightTypeInformation)
    this.lastRightValue = this.getRuntimeContext.getState(rightValueDescriptor)
  }

  override def getProducedType: TypeInformation[RecordWrapper[TOut, TKey]] =
    RecordWrapperTypeInformation.wrap(this.outputTypeInformation, this.keyTypeInformation)
}
