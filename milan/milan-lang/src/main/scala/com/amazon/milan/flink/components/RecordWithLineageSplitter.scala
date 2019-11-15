package com.amazon.milan.flink.components

import com.amazon.milan.types.{LineageRecord, Record, RecordWithLineage}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.{Collector, OutputTag}


class RecordWithLineageSplitter[T <: Record](val lineageOutputTag: OutputTag[LineageRecord],
                                             val recordTypeInformation: TypeInformation[T])
  extends ProcessFunction[RecordWithLineage[T], T]
    with ResultTypeQueryable[T] {

  override def processElement(value: RecordWithLineage[T],
                              context: ProcessFunction[RecordWithLineage[T], T]#Context,
                              collector: Collector[T]): Unit = {
    collector.collect(value.record)
    context.output(this.lineageOutputTag, value.lineage)
  }

  override def getProducedType: TypeInformation[T] = this.recordTypeInformation
}
