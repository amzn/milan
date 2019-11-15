package com.amazon.milan.flink.components

import com.amazon.milan.flink.compiler.internal.LineageRecordFactory
import com.amazon.milan.types.{Record, RecordWithLineage}
import org.apache.flink.api.common.functions.{RichMapFunction, RuntimeContext}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration


/**
 * Wrapper for Flink [[RichMapFunction]] operators that outputs lineage information.
 */
class LineageMapFunctionWrapper[TIn <: Record, TOut <: Record, TMap <: RichMapFunction[TIn, TOut] with ResultTypeQueryable[TOut]](wrapped: TMap, lineageRecordFactory: LineageRecordFactory)
  extends RichMapFunction[TIn, RecordWithLineage[TOut]]
    with ResultTypeQueryable[RecordWithLineage[TOut]] {

  private val outputType = new RecordWithLineageTypeInformation[TOut](this.wrapped.getProducedType)

  override def map(value: TIn): RecordWithLineage[TOut] = {
    val mapped = this.wrapped.map(value)
    val source = this.lineageRecordFactory.createRecordPointer(value.getRecordId)
    val lineage = this.lineageRecordFactory.createLineageRecord(mapped.getRecordId, Seq(source))
    RecordWithLineage(mapped, lineage)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    this.wrapped.open(parameters)
  }

  override def setRuntimeContext(context: RuntimeContext): Unit = {
    super.setRuntimeContext(context)
    this.wrapped.setRuntimeContext(context)
  }

  override def getProducedType: TypeInformation[RecordWithLineage[TOut]] = this.outputType
}
