package com.amazon.milan.flink.runtime

import com.amazon.milan.flink.internal.JoinLineageRecordFactory
import com.amazon.milan.flink.metrics.MetricFactory
import com.amazon.milan.flink.types.{RecordWrapper, RecordWrapperTypeInformation}
import com.amazon.milan.types.LineageRecord
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.{Collector, OutputTag}


object LeftJoinCoProcessFunction {
  val LeftInputRecordsCounterMetricName = "left_input_record_count"
  val RightInputRecordsCounterMetricName = "right_input_record_count"
  val OutputRecordsCounterMetricName = "output_record_count"
}

import com.amazon.milan.flink.runtime.LeftJoinCoProcessFunction._


abstract class LeftJoinKeyedCoProcessFunction[TLeft >: Null, TRight >: Null, TKey >: Null <: Product, TOut >: Null](rightTypeInformation: TypeInformation[TRight],
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

  @transient private lazy val canProduceLineage = leftRecordIdExtractor.canExtractRecordId && rightRecordIdExtractor.canExtractRecordId && outputRecordIdExtractor.canExtractRecordId
  @transient private lazy val leftInputRecordsCounter = this.metricFactory.createCounter(this.getRuntimeContext, LeftInputRecordsCounterMetricName)
  @transient private lazy val rightInputRecordsCounter = this.metricFactory.createCounter(this.getRuntimeContext, RightInputRecordsCounterMetricName)
  @transient private lazy val outputRecordsCounter = this.metricFactory.createCounter(this.getRuntimeContext, OutputRecordsCounterMetricName)

  @transient private var lastRightValue: ValueState[TRight] = _

  protected def map(left: TLeft, right: TRight): TOut

  protected def postCondition(left: TLeft, right: TRight): Boolean

  override def processElement1(leftRecord: RecordWrapper[TLeft, TKey],
                               context: KeyedCoProcessFunction[TKey, RecordWrapper[TLeft, TKey], RecordWrapper[TRight, TKey], RecordWrapper[TOut, TKey]]#Context,
                               collector: Collector[RecordWrapper[TOut, TKey]]): Unit = {
    this.leftInputRecordsCounter.increment()

    val leftValue = leftRecord.value
    val rightValue = this.lastRightValue.value()

    if (this.postCondition(leftValue, rightValue)) {
      val output = this.map(leftValue, rightValue)

      if (output != null) {
        if (this.canProduceLineage) {
          val lineageRecord = this.createLineageRecord(this.outputRecordIdExtractor(output), leftValue, rightValue)
          context.output(this.lineageOutputTag, lineageRecord)
        }

        collector.collect(RecordWrapper.wrap[TOut, TKey](output, leftRecord.key, 0))
        this.outputRecordsCounter.increment()
      }
    }
  }

  override def processElement2(rightRecord: RecordWrapper[TRight, TKey],
                               context: KeyedCoProcessFunction[TKey, RecordWrapper[TLeft, TKey], RecordWrapper[TRight, TKey], RecordWrapper[TOut, TKey]]#Context,
                               collector: Collector[RecordWrapper[TOut, TKey]]): Unit = {
    this.rightInputRecordsCounter.increment()
    this.lastRightValue.update(rightRecord.value)
  }

  override def open(parameters: Configuration): Unit = {
    val rightValueDescriptor = new ValueStateDescriptor[TRight]("lastRightValue", this.rightTypeInformation)
    this.lastRightValue = this.getRuntimeContext.getState(rightValueDescriptor)
  }

  override def getProducedType: TypeInformation[RecordWrapper[TOut, TKey]] =
    RecordWrapperTypeInformation.wrap(this.outputTypeInformation, this.keyTypeInformation)

  private def createLineageRecord(outputRecordId: String, leftRecord: TLeft, rightRecord: TRight): LineageRecord = {
    val sourceRecords =
      Option(leftRecord).toSeq.map(r => this.lineageRecordFactory.createLeftRecordPointer(this.leftRecordIdExtractor(r))) ++
        Option(rightRecord).toSeq.map(r => this.lineageRecordFactory.createRightRecordPointer(this.rightRecordIdExtractor(r)))

    this.lineageRecordFactory.createLineageRecord(outputRecordId, sourceRecords)
  }
}
