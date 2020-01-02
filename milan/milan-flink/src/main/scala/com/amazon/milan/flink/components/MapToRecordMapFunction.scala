package com.amazon.milan.flink.components

import com.amazon.milan.flink.compiler.internal.RuntimeCompiledFunction
import com.amazon.milan.flink.metrics.MetricFactory
import com.amazon.milan.program.MapRecord
import com.amazon.milan.typeutil.TypeDescriptor
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.slf4j.LoggerFactory


object MapToRecordMapFunction {
  val ProcessedRecordsCounterMetricName = "processed_record_count"

  val typeName: String = getClass.getTypeName.stripSuffix("$")
}

import com.amazon.milan.flink.components.MapToRecordMapFunction._


class MapToRecordMapFunction[TIn, TOut](mapExpr: MapRecord,
                                        inputRecordType: TypeDescriptor[_],
                                        outputTypeInformation: TypeInformation[TOut],
                                        metricFactory: MetricFactory)
  extends RichMapFunction[TIn, TOut]
    with ResultTypeQueryable[TOut]
    with Serializable {

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))
  @transient private lazy val processedRecordsCounter = this.metricFactory.createCounter(this.getRuntimeContext, ProcessedRecordsCounterMetricName)

  private val compiledMapFunction = new RuntimeCompiledFunction[TIn, TOut](this.inputRecordType, this.mapExpr.expr)

  override def map(input: TIn): TOut = {
    this.processedRecordsCounter.increment()
    this.compiledMapFunction(input)
  }

  override def getProducedType: TypeInformation[TOut] = this.outputTypeInformation
}
