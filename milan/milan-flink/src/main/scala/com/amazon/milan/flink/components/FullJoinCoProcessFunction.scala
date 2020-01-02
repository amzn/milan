package com.amazon.milan.flink.components

import com.amazon.milan.Id
import com.amazon.milan.flink.compiler.internal.{ConstantFunction2, JoinLineageRecordFactory, RecordIdExtractorFactory, RuntimeCompiledFunction2}
import com.amazon.milan.flink.metrics.MetricFactory
import com.amazon.milan.flink.types.ArrayRecord
import com.amazon.milan.program._
import com.amazon.milan.types.{LineageRecord, RecordWithLineage}
import com.amazon.milan.typeutil.TypeDescriptor
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory


object FullJoinCoProcessFunction {
  val LeftInputRecordsCounterMetricName = "left_input_record_count"
  val RightInputRecordsCounterMetricName = "right_input_record_count"
  val OutputRecordsCounterMetricName = "output_record_count"
}

import com.amazon.milan.flink.components.FullJoinCoProcessFunction._


/**
 * Flink [[CoProcessFunction]] base class for full joins.
 *
 * @param leftInputType         Type descriptor of the left input type.
 * @param rightInputType        Type descriptor of the right input type.
 * @param leftTypeInformation   [[TypeInformation]] for the left input stream.
 * @param rightTypeInformation  [[TypeInformation]] for the right input stream.
 * @param outputTypeInformation [[TypeInformation]] for the output stream.
 * @tparam TLeft  The left input stream record type.
 * @tparam TRight The right input stream record type.
 * @tparam TOut   The output record type.
 */
abstract class FullJoinCoProcessFunction[TLeft, TRight, TOut](leftInputType: TypeDescriptor[TLeft],
                                                              rightInputType: TypeDescriptor[TRight],
                                                              outputType: TypeDescriptor[TOut],
                                                              leftTypeInformation: TypeInformation[TLeft],
                                                              rightTypeInformation: TypeInformation[TRight],
                                                              outputTypeInformation: TypeInformation[TOut],
                                                              joinPostConditions: Option[FunctionDef],
                                                              lineageFactory: JoinLineageRecordFactory,
                                                              metricFactory: MetricFactory)
  extends CoProcessFunction[TLeft, TRight, RecordWithLineage[TOut]]
    with ResultTypeQueryable[RecordWithLineage[TOut]] {

  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  @transient private lazy val getLeftRecordId = RecordIdExtractorFactory.getRecordIdExtractor(this.leftInputType)
  @transient private lazy val getRightRecordId = RecordIdExtractorFactory.getRecordIdExtractor(this.rightInputType)
  @transient private lazy val getOutputRecordId = RecordIdExtractorFactory.getRecordIdExtractor(this.outputType)
  @transient private lazy val canProduceLineage = getLeftRecordId.isDefined && getRightRecordId.isDefined && getOutputRecordId.isDefined

  @transient private lazy val leftInputRecordsCounter = this.metricFactory.createCounter(this.getRuntimeContext, LeftInputRecordsCounterMetricName)
  @transient private lazy val rightInputRecordsCounter = this.metricFactory.createCounter(this.getRuntimeContext, RightInputRecordsCounterMetricName)
  @transient private lazy val outputRecordsCounter = this.metricFactory.createCounter(this.getRuntimeContext, OutputRecordsCounterMetricName)

  @transient private var lastRightValue: ValueState[TRight] = _
  @transient private var lastLeftValue: ValueState[TLeft] = _

  private val producedType = new RecordWithLineageTypeInformation[TOut](this.outputTypeInformation)

  private val compiledPostCondition =
    this.joinPostConditions match {
      case Some(f) =>
        new RuntimeCompiledFunction2[TLeft, TRight, Boolean](
          this.leftInputType,
          this.rightInputType,
          f)

      case None =>
        new ConstantFunction2[TLeft, TRight, Boolean](true)
    }

  protected def map(left: TLeft, right: TRight): TOut

  override def processElement1(leftValue: TLeft,
                               context: CoProcessFunction[TLeft, TRight, RecordWithLineage[TOut]]#Context,
                               collector: Collector[RecordWithLineage[TOut]]): Unit = {
    this.logger.info("Got left value.")
    this.leftInputRecordsCounter.increment()

    this.lastLeftValue.update(leftValue)
    val rightValue = this.lastRightValue.value()

    if (this.compiledPostCondition(leftValue, rightValue)) {
      val output = this.map(leftValue, rightValue)
      if (output != null) {
        collector.collect(this.addLineage(output, leftValue, rightValue))
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
    val leftValue = this.lastLeftValue.value()

    if (this.compiledPostCondition(leftValue, rightValue)) {
      val output = this.map(leftValue, rightValue)
      if (output != null) {
        collector.collect(this.addLineage(output, leftValue, rightValue))
        this.outputRecordsCounter.increment()
      }
    }
  }

  override def open(parameters: Configuration): Unit = {
    val leftValueDescriptor = new ValueStateDescriptor[TLeft]("lastLeftValue", this.leftTypeInformation)
    this.lastLeftValue = this.getRuntimeContext.getState(leftValueDescriptor)

    val rightValueDescriptor = new ValueStateDescriptor[TRight]("lastRightValue", this.rightTypeInformation)
    this.lastRightValue = this.getRuntimeContext.getState(rightValueDescriptor)
  }

  override def getProducedType: TypeInformation[RecordWithLineage[TOut]] = this.producedType

  private def addLineage(outputRecord: TOut, leftRecord: TLeft, rightRecord: TRight): RecordWithLineage[TOut] = {
    if (canProduceLineage) {
      val lineage = this.createLineageRecord(this.getOutputRecordId.get(outputRecord), leftRecord, rightRecord)
      RecordWithLineage(outputRecord, lineage)
    }
    else {
      RecordWithLineage(outputRecord, null)
    }
  }

  private def createLineageRecord(outputRecordId: String, leftRecord: TLeft, rightRecord: TRight): LineageRecord = {
    val sourceRecords =
      Option(leftRecord).toSeq.map(r => this.lineageFactory.createLeftRecordPointer(this.getLeftRecordId.get(r))) ++
        Option(rightRecord).toSeq.map(r => this.lineageFactory.createRightRecordPointer(this.getRightRecordId.get(r)))

    this.lineageFactory.createLineageRecord(outputRecordId, sourceRecords)
  }
}


/**
 * Flink [[CoProcessFunction]] class for full joins where the output of the select statement is a single object.
 *
 * @param mapExpr               The map function definition.
 * @param leftInputType         Type descriptor of the left input type.
 * @param rightInputType        Type descriptor of the right input type.
 * @param leftTypeInformation   [[TypeInformation]] for the left input stream.
 * @param rightTypeInformation  [[TypeInformation]] for the right input stream.
 * @param outputTypeInformation [[TypeInformation]] for the output stream.
 * @tparam TLeft  The left input stream record type.
 * @tparam TRight The right input stream record type.
 * @tparam TOut   The output record type.
 */
class FullJoinMapToRecordCoProcessFunction[TLeft, TRight, TOut](mapExpr: MapRecord,
                                                                leftInputType: TypeDescriptor[TLeft],
                                                                rightInputType: TypeDescriptor[TRight],
                                                                leftTypeInformation: TypeInformation[TLeft],
                                                                rightTypeInformation: TypeInformation[TRight],
                                                                outputTypeInformation: TypeInformation[TOut],
                                                                joinPostConditions: Option[FunctionDef],
                                                                lineageFactory: JoinLineageRecordFactory,
                                                                metricFactory: MetricFactory)
  extends FullJoinCoProcessFunction[TLeft, TRight, TOut](
    leftInputType,
    rightInputType,
    mapExpr.recordType.asInstanceOf[TypeDescriptor[TOut]],
    leftTypeInformation,
    rightTypeInformation,
    outputTypeInformation,
    joinPostConditions,
    lineageFactory,
    metricFactory) {

  private val compiledMapFunction = new RuntimeCompiledFunction2[TLeft, TRight, TOut](
    this.leftInputType,
    this.rightInputType,
    this.mapExpr.expr)

  override protected def map(left: TLeft, right: TRight): TOut = this.compiledMapFunction(left, right)
}


/**
 * Flink [[CoProcessFunction]] class for full joins where the output of the select statement is a set of fields.
 *
 * @param mapExpr               The map function definition.
 * @param leftInputType         Type descriptor of the left input type.
 * @param rightInputType        Type descriptor of the right input type.
 * @param leftTypeInformation   [[TypeInformation]] for the left input stream.
 * @param rightTypeInformation  [[TypeInformation]] for the right input stream.
 * @param outputTypeInformation [[TypeInformation]] for the output stream.
 * @tparam TLeft  The left input stream record type.
 * @tparam TRight The right input stream record type.
 */
class FullJoinMapToFieldsCoProcessFunction[TLeft, TRight](mapExpr: MapFields,
                                                          leftInputType: TypeDescriptor[TLeft],
                                                          rightInputType: TypeDescriptor[TRight],
                                                          leftTypeInformation: TypeInformation[TLeft],
                                                          rightTypeInformation: TypeInformation[TRight],
                                                          outputTypeInformation: TupleStreamTypeInformation,
                                                          joinPostConditions: Option[FunctionDef],
                                                          lineageFactory: JoinLineageRecordFactory,
                                                          metricFactory: MetricFactory)
  extends FullJoinCoProcessFunction[TLeft, TRight, ArrayRecord](
    leftInputType,
    rightInputType,
    mapExpr.recordType.asInstanceOf[TypeDescriptor[ArrayRecord]],
    leftTypeInformation,
    rightTypeInformation,
    outputTypeInformation,
    joinPostConditions,
    lineageFactory,
    metricFactory) {

  private val compiledFieldFunctions = this.mapExpr.fields.map(f =>
    new RuntimeCompiledFunction2[TLeft, TRight, Any](this.leftInputType, this.rightInputType, f.expr)
  ).toArray

  override protected def map(left: TLeft, right: TRight): ArrayRecord = {
    val values = this.compiledFieldFunctions.map(f => f(left, right))
    ArrayRecord(Id.newId(), values)
  }
}
