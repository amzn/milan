package com.amazon.milan.flink.runtime

import com.amazon.milan.flink.internal.JoinLineageRecordFactory
import com.amazon.milan.flink.metrics.MetricFactory
import com.amazon.milan.flink.runtime.LeftJoinCoProcessFunction._
import com.amazon.milan.flink.types.{RecordWrapper, RecordWrapperTypeInformation}
import com.amazon.milan.types.LineageRecord
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.{Collector, OutputTag}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


abstract class LeftInnerJoinKeyedCoProcessFunction[TLeft >: Null, TRight >: Null, TKey >: Null <: Product, TOut >: Null](leftTypeInformation: TypeInformation[TLeft],
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

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(this.getClass))

  @transient private lazy val canProduceLineage = leftRecordIdExtractor.canExtractRecordId && rightRecordIdExtractor.canExtractRecordId && outputRecordIdExtractor.canExtractRecordId
  @transient private lazy val leftInputRecordsCounter = this.metricFactory.createCounter(this.getRuntimeContext, LeftInputRecordsCounterMetricName)
  @transient private lazy val rightInputRecordsCounter = this.metricFactory.createCounter(this.getRuntimeContext, RightInputRecordsCounterMetricName)
  @transient private lazy val outputRecordsCounter = this.metricFactory.createCounter(this.getRuntimeContext, OutputRecordsCounterMetricName)

  @transient private var leftValues: ListState[TLeft] = _
  @transient private var lastRightValue: ValueState[TRight] = _

  protected def map(left: TLeft, right: TRight): TOut

  protected def postCondition(left: TLeft, right: TRight): Boolean

  override def processElement1(leftRecord: RecordWrapper[TLeft, TKey],
                               context: KeyedCoProcessFunction[TKey, RecordWrapper[TLeft, TKey], RecordWrapper[TRight, TKey], RecordWrapper[TOut, TKey]]#Context,
                               collector: Collector[RecordWrapper[TOut, TKey]]): Unit = {
    this.leftInputRecordsCounter.increment()

    val leftValue = leftRecord.value
    val rightValue = this.lastRightValue.value()

    // If this left value matches the previous right value then join and output the result,
    // otherwise store the left value for later.
    if (!this.postCondition(leftValue, rightValue)) {
      this.leftValues.add(leftValue)
      this.logger.debug(s"Cache size for key '${context.getCurrentKey}' = ${this.leftValues.get.asScala.size}")
    }
    else {
      this.joinAndCollect(leftValue, rightValue, leftRecord.key, context, collector)
    }
  }

  override def processElement2(rightRecord: RecordWrapper[TRight, TKey],
                               context: KeyedCoProcessFunction[TKey, RecordWrapper[TLeft, TKey], RecordWrapper[TRight, TKey], RecordWrapper[TOut, TKey]]#Context,
                               collector: Collector[RecordWrapper[TOut, TKey]]): Unit = {
    this.rightInputRecordsCounter.increment()

    val rightValue = rightRecord.value
    this.lastRightValue.update(rightValue)

    // If the new right value matches any of the left values that are waiting for a match then join and output those
    // matches.
    var remainingLeftValues = List.empty[TLeft]
    val startingCacheSize = this.leftValues.get.asScala.size

    this.leftValues.get().asScala.foreach(leftValue =>
      if (this.postCondition(leftValue, rightValue)) {
        this.logger.debug(s"Found match for key '${context.getCurrentKey}' in cache.")
        this.joinAndCollect(leftValue, rightValue, rightRecord.key, context, collector)
      }
      else {
        remainingLeftValues = remainingLeftValues :+ leftValue
      }
    )

    this.leftValues.update(remainingLeftValues.asJava)

    val finalCacheSize = remainingLeftValues.length
    if (finalCacheSize != startingCacheSize) {
      this.logger.debug(s"Cache size for key '${context.getCurrentKey}' = $finalCacheSize")
    }
  }

  override def open(parameters: Configuration): Unit = {
    val rightValueDescriptor = new ValueStateDescriptor[TRight]("lastRightValue", this.rightTypeInformation)
    this.lastRightValue = this.getRuntimeContext.getState(rightValueDescriptor)

    val leftValuesDescriptor = new ListStateDescriptor[TLeft]("leftValues", this.leftTypeInformation)
    this.leftValues = this.getRuntimeContext.getListState(leftValuesDescriptor)
  }

  override def getProducedType: TypeInformation[RecordWrapper[TOut, TKey]] =
    RecordWrapperTypeInformation.wrap(this.outputTypeInformation, this.keyTypeInformation)

  private def joinAndCollect(leftValue: TLeft,
                             rightValue: TRight,
                             key: TKey,
                             context: KeyedCoProcessFunction[TKey, RecordWrapper[TLeft, TKey], RecordWrapper[TRight, TKey], RecordWrapper[TOut, TKey]]#Context,
                             collector: Collector[RecordWrapper[TOut, TKey]]): Unit = {
    val output = this.map(leftValue, rightValue)

    if (output != null) {
      if (this.canProduceLineage) {
        val lineageRecord = this.createLineageRecord(this.outputRecordIdExtractor(output), leftValue, rightValue)
        context.output(this.lineageOutputTag, lineageRecord)
      }

      collector.collect(RecordWrapper.wrap[TOut, TKey](output, key, 0))
      this.outputRecordsCounter.increment()
    }
  }

  private def createLineageRecord(outputRecordId: String, leftRecord: TLeft, rightRecord: TRight): LineageRecord = {
    val sourceRecords =
      Option(leftRecord).toSeq.map(r => this.lineageRecordFactory.createLeftRecordPointer(this.leftRecordIdExtractor(r))) ++
        Option(rightRecord).toSeq.map(r => this.lineageRecordFactory.createRightRecordPointer(this.rightRecordIdExtractor(r)))

    this.lineageRecordFactory.createLineageRecord(outputRecordId, sourceRecords)
  }
}
