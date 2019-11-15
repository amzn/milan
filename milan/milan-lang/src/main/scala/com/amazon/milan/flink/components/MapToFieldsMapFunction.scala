package com.amazon.milan.flink.components

import com.amazon.milan.Id
import com.amazon.milan.flink.compiler.internal.RuntimeCompiledFunction
import com.amazon.milan.flink.metrics.MetricFactory
import com.amazon.milan.flink.types.ArrayRecord
import com.amazon.milan.program.MapFields
import com.amazon.milan.typeutil.TypeDescriptor
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.slf4j.LoggerFactory


object MapToFieldsMapFunction {
  val ProcessedRecordsCounterMetricName = "processed_record_count"
}

import com.amazon.milan.flink.components.MapToFieldsMapFunction._


class MapToFieldsMapFunction[TIn](mapExpr: MapFields,
                                  inputRecordType: TypeDescriptor[_],
                                  outputTypeInformation: TupleStreamTypeInformation,
                                  metricFactory: MetricFactory)
  extends RichMapFunction[TIn, ArrayRecord]
    with ResultTypeQueryable[ArrayRecord]
    with Serializable {

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))
  @transient private lazy val processedRecordsCounter = this.metricFactory.createCounter(this.getRuntimeContext, ProcessedRecordsCounterMetricName)

  private val compiledFieldFunctions =
    this.mapExpr.fields.map(f =>
      new RuntimeCompiledFunction[TIn, Any](this.inputRecordType, f.expr)
    ).toArray

  override def map(input: TIn): ArrayRecord = {
    this.processedRecordsCounter.increment()

    val values = this.compiledFieldFunctions.map(f => f(input))
    ArrayRecord(Id.newId(), values)
  }

  override def getProducedType: TypeInformation[ArrayRecord] = this.outputTypeInformation
}
