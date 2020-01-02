package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.Id
import com.amazon.milan.flink._
import com.amazon.milan.flink.components.{RecordWithLineageSplitter, RecordWithLineageTypeInformation}
import com.amazon.milan.types.{LineageRecord, RecordWithLineage}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
import org.apache.flink.util.OutputTag


object LineageSplitterFactory {
  private val typeName: String = getClass.getTypeName.stripSuffix("$")

  /**
   * Splits a [[SingleOutputStreamOperator]] of records with associated lineage into separate streams of records and lineage records.
   *
   * @param dataStream The data stream to split.
   * @return A tuple of the stream of records and the stream of lineage records.
   */
  def splitStream(dataStream: SingleOutputStreamOperator[RecordWithLineage[_]]): (SingleOutputStreamOperator[_], DataStream[LineageRecord]) = {
    val recordTypeInfo = dataStream.getType.asInstanceOf[RecordWithLineageTypeInformation[_]].recordTypeInformation

    val eval = RuntimeEvaluator.instance
    val inputTypeName = recordTypeInfo.getTypeName
    eval.evalFunction[SingleOutputStreamOperator[RecordWithLineage[_]], (SingleOutputStreamOperator[_], DataStream[LineageRecord])](
      "dataStream",
      FlinkTypeNames.singleOutputStreamOperator(s"${RecordWithLineage.typeName}[$inputTypeName]"),
      s"${this.typeName}.splitStreamImpl[$inputTypeName](dataStream)",
      dataStream)
  }

  /**
   * Splits a [[SingleOutputStreamOperator]] of records with associated lineage into separate streams of records and lineage records.
   *
   * @param dataStream The data stream to split.
   * @return A tuple of the stream of records and the stream of lineage records.
   */
  def splitStreamImpl[T](dataStream: SingleOutputStreamOperator[RecordWithLineage[T]]): (SingleOutputStreamOperator[T], DataStream[LineageRecord]) = {
    val recordTypeInfo = dataStream.getType.asInstanceOf[RecordWithLineageTypeInformation[T]].recordTypeInformation

    val lineageOutputTag = new OutputTag[LineageRecord](Id.newId(), createTypeInformation[LineageRecord])

    val splitter = new RecordWithLineageSplitter[T](lineageOutputTag, recordTypeInfo)
    val name = s"LineageSplit [${dataStream.getName}]"
    val output = dataStream.process(splitter).name(name)

    val lineageOutput = output.getSideOutput(lineageOutputTag)
    (output, lineageOutput)
  }
}
