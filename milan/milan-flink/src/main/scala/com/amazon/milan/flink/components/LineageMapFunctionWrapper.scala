package com.amazon.milan.flink.components

import com.amazon.milan.flink.compiler.internal.{LineageRecordFactory, RecordIdExtractorFactory}
import com.amazon.milan.types.RecordWithLineage
import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.flink.api.common.functions.{RichMapFunction, RuntimeContext}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration


/**
 * Wrapper for Flink [[RichMapFunction]] operators that outputs lineage information.
 */
class LineageMapFunctionWrapper[TIn, TOut, TMap <: RichMapFunction[TIn, TOut] with ResultTypeQueryable[TOut]](inputRecordType: TypeDescriptor[TIn],
                                                                                                              outputRecordType: TypeDescriptor[TOut],
                                                                                                              wrapped: TMap,
                                                                                                              lineageRecordFactory: LineageRecordFactory)
  extends RichMapFunction[TIn, RecordWithLineage[TOut]]
    with ResultTypeQueryable[RecordWithLineage[TOut]] {

  @transient private lazy val getInputRecordId = RecordIdExtractorFactory.getRecordIdExtractor(this.inputRecordType)
  @transient private lazy val getOutputRecordId = RecordIdExtractorFactory.getRecordIdExtractor(this.outputRecordType)
  @transient private lazy val canProduceLineage = getInputRecordId.isDefined && getOutputRecordId.isDefined

  private val outputType = new RecordWithLineageTypeInformation[TOut](this.wrapped.getProducedType)

  override def map(value: TIn): RecordWithLineage[TOut] = {
    val mapped = this.wrapped.map(value)

    val lineage =
      if (this.canProduceLineage) {
        val source = this.lineageRecordFactory.createRecordPointer(this.getInputRecordId.get(value))
        this.lineageRecordFactory.createLineageRecord(this.getOutputRecordId.get(mapped), Seq(source))
      }
      else {
        null
      }

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
