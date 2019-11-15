package com.amazon.milan.flink.components

import com.amazon.milan.Id
import com.amazon.milan.flink.compiler.internal.{ConstantFunction2, JoinLineageRecordFactory, RuntimeCompiledFunction2}
import com.amazon.milan.flink.metrics.MetricFactory
import com.amazon.milan.flink.types.ArrayRecord
import com.amazon.milan.program.{FunctionDef, MapFields, MapRecord}
import com.amazon.milan.types.{LineageRecord, Record, RecordWithLineage}
import com.amazon.milan.typeutil.TypeDescriptor
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory


object LeftJoinCoProcessFunction {
  val LeftInputRecordsCounterMetricName = "left_input_record_count"
  val RightInputRecordsCounterMetricName = "right_input_record_count"
  val OutputRecordsCounterMetricName = "output_record_count"
}

import com.amazon.milan.flink.components.LeftJoinCoProcessFunction._


abstract class LeftJoinCoProcessFunction[TLeft <: Record, TRight <: Record, TOut <: Record](leftInputType: TypeDescriptor[_],
                                                                                            rightInputType: TypeDescriptor[_],
                                                                                            rightTypeInformation: TypeInformation[TRight],
                                                                                            outputTypeInformation: TypeInformation[TOut],
                                                                                            joinPostConditions: Option[FunctionDef],
                                                                                            lineageRecordFactory: JoinLineageRecordFactory,
                                                                                            metricFactory: MetricFactory)
  extends CoProcessFunction[TLeft, TRight, RecordWithLineage[TOut]]
    with ResultTypeQueryable[RecordWithLineage[TOut]] {

  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  @transient private lazy val leftInputRecordsCounter = this.metricFactory.createCounter(this.getRuntimeContext, LeftInputRecordsCounterMetricName)
  @transient private lazy val rightInputRecordsCounter = this.metricFactory.createCounter(this.getRuntimeContext, RightInputRecordsCounterMetricName)
  @transient private lazy val outputRecordsCounter = this.metricFactory.createCounter(this.getRuntimeContext, OutputRecordsCounterMetricName)

  @transient private var lastRightValue: ValueState[TRight] = _

  private val producedType = new RecordWithLineageTypeInformation[TOut](this.outputTypeInformation)

  private val compiledPostCondition = this.joinPostConditions match {
    case Some(f) => new RuntimeCompiledFunction2[TLeft, TRight, Boolean](this.leftInputType, this.rightInputType, f)
    case None => new ConstantFunction2[TLeft, TRight, Boolean](true)
  }

  protected def map(left: TLeft, right: TRight): TOut

  override def processElement1(leftValue: TLeft,
                               context: CoProcessFunction[TLeft, TRight, RecordWithLineage[TOut]]#Context,
                               collector: Collector[RecordWithLineage[TOut]]): Unit = {
    this.logger.info("Got left value.")
    this.leftInputRecordsCounter.increment()

    val rightValue = this.lastRightValue.value()

    if (this.compiledPostCondition(leftValue, rightValue)) {
      val output = this.map(leftValue, rightValue)
      if (output != null) {
        val lineage = this.createLineageRecord(output.getRecordId, leftValue, rightValue)
        collector.collect(RecordWithLineage(output, lineage))
        this.outputRecordsCounter.increment()
      }
    }
  }

  override def processElement2(rightValue: TRight,
                               context: CoProcessFunction[TLeft, TRight, RecordWithLineage[TOut]]#Context,
                               collector: Collector[RecordWithLineage[TOut]]): Unit = {
    this.logger.info("Got right value.")
    this.rightInputRecordsCounter.increment()
    this.lastRightValue.update(rightValue)
  }

  override def open(parameters: Configuration): Unit = {
    val rightValueDescriptor = new ValueStateDescriptor[TRight]("lastRightValue", this.rightTypeInformation)
    this.lastRightValue = this.getRuntimeContext.getState(rightValueDescriptor)
  }

  override def getProducedType: TypeInformation[RecordWithLineage[TOut]] = this.producedType

  private def createLineageRecord(outputRecordId: String, leftRecord: TLeft, rightRecord: TRight): LineageRecord = {
    val sourceRecords =
      Option(leftRecord).toSeq.map(r => this.lineageRecordFactory.createLeftRecordPointer(r.getRecordId)) ++
        Option(rightRecord).toSeq.map(r => this.lineageRecordFactory.createRightRecordPointer(r.getRecordId))

    this.lineageRecordFactory.createLineageRecord(outputRecordId, sourceRecords)
  }
}


class LeftJoinMapToRecordCoProcessFunction[TLeft <: Record, TRight <: Record, TOut <: Record](mapExpr: MapRecord,
                                                                                              leftInputType: TypeDescriptor[_],
                                                                                              rightInputType: TypeDescriptor[_],
                                                                                              rightTypeInformation: TypeInformation[TRight],
                                                                                              outputTypeInformation: TypeInformation[TOut],
                                                                                              joinPostConditions: Option[FunctionDef],
                                                                                              lineageRecordFactory: JoinLineageRecordFactory,
                                                                                              metricFactory: MetricFactory)
  extends LeftJoinCoProcessFunction[TLeft, TRight, TOut](
    leftInputType,
    rightInputType,
    rightTypeInformation,
    outputTypeInformation,
    joinPostConditions,
    lineageRecordFactory,
    metricFactory) {

  private val compiledMapFunction = new RuntimeCompiledFunction2[TLeft, TRight, TOut](this.leftInputType, this.rightInputType, this.mapExpr.expr)

  override protected def map(left: TLeft, right: TRight): TOut = this.compiledMapFunction(left, right)
}


class LeftJoinMapToFieldsCoProcessFunction[TLeft <: Record, TRight <: Record](mapExpr: MapFields,
                                                                              leftInputType: TypeDescriptor[_],
                                                                              rightInputType: TypeDescriptor[_],
                                                                              rightTypeInformation: TypeInformation[TRight],
                                                                              outputTypeInformation: TupleStreamTypeInformation,
                                                                              joinPostConditions: Option[FunctionDef],
                                                                              lineageRecordFactory: JoinLineageRecordFactory,
                                                                              metricFactory: MetricFactory)
  extends LeftJoinCoProcessFunction[TLeft, TRight, ArrayRecord](
    leftInputType,
    rightInputType,
    rightTypeInformation,
    outputTypeInformation,
    joinPostConditions,
    lineageRecordFactory,
    metricFactory) {

  private val compiledFieldFunctions = this.mapExpr.fields.map(f =>
    new RuntimeCompiledFunction2[TLeft, TRight, Any](this.leftInputType, this.rightInputType, f.expr)
  ).toArray

  override protected def map(left: TLeft, right: TRight): ArrayRecord = {
    val values = this.compiledFieldFunctions.map(f => f(left, right))
    ArrayRecord(Id.newId(), values)
  }
}
