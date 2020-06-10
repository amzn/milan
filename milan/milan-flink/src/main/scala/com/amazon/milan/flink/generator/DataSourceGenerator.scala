package com.amazon.milan.flink.generator

import com.amazon.milan.application.DataSource
import com.amazon.milan.application.sources.{FileDataSource, KinesisDataSource, ListDataSource}
import com.amazon.milan.flink.runtime.{DelayedListSourceFunction, WrapRecordsMapFunction}
import com.amazon.milan.serialization.MilanObjectMapper
import com.amazon.milan.testing.DelayedListDataSource
import com.amazon.milan.typeutil.types


trait DataSourceGenerator {
  val typeLifter: TypeLifter

  import typeLifter._

  def addDataSource(outputs: GeneratorOutputs,
                    source: DataSource[_],
                    outputStreamVal: ValName,
                    streamIdentifier: String): GeneratedUnkeyedDataStream = {
    source match {
      case listDataSource: ListDataSource[_] =>
        this.addListDataSource(outputs, listDataSource, outputStreamVal, streamIdentifier)

      case fileDataSource: FileDataSource[_] =>
        this.addFileDataSource(outputs, fileDataSource, outputStreamVal, streamIdentifier)

      case delayedListDataSource: DelayedListDataSource[_] =>
        this.addDelayedListDataSource(outputs, delayedListDataSource, outputStreamVal, streamIdentifier)

      case kinesisDataSource: KinesisDataSource[_] =>
        this.addKinesisDataSource(outputs, kinesisDataSource, outputStreamVal, streamIdentifier)
    }
  }

  private def addListDataSource(outputs: GeneratorOutputs,
                                source: ListDataSource[_],
                                outputStreamVal: ValName,
                                streamIdentifier: String): GeneratedUnkeyedDataStream = {
    val recordTypeInfoVal = outputs.newValName(s"${outputStreamVal}_typeinfo_")
    val valuesVal = outputs.newValName(s"${outputStreamVal}_values_")

    val sourceJson = MilanObjectMapper.writeValueAsString(source.values)

    val dataStreamVal = outputs.newValName(s"${outputStreamVal}_records_")
    val recordType = source.getGenericArguments.head

    val code =
      q"""
         |val $recordTypeInfoVal = ${liftTypeDescriptorToTypeInformation(recordType)}
         |val $valuesVal = com.amazon.milan.flink.runtime.RuntimeUtil.loadJsonList[${recordType.toTerm}]($sourceJson)
         |val $dataStreamVal = com.amazon.milan.flink.runtime.DataSourceUtil.addListDataSource[${recordType.toTerm}](
         |  ${outputs.streamEnvVal},
         |  $valuesVal,
         |  ${source.runForever},
         |  $recordTypeInfoVal)
         |val $outputStreamVal = $dataStreamVal.map(new ${nameOf[WrapRecordsMapFunction[Any]]}[${recordType.toTerm}]($recordTypeInfoVal))
         |""".strip

    outputs.appendMain(code)

    GeneratedUnkeyedDataStream(streamIdentifier, outputStreamVal, recordType, types.EmptyTuple, isContextual = false)
  }

  private def addFileDataSource(outputs: GeneratorOutputs,
                                source: FileDataSource[_],
                                outputStreamVal: ValName,
                                streamIdentifier: String): GeneratedUnkeyedDataStream = {
    val recordType = source.getGenericArguments.head
    val recordTypeInfoVal = outputs.newValName(s"${outputStreamVal}_typeinfo_")
    val dataStreamVal = outputs.newValName(s"${outputStreamVal}_records_")

    val code =
      q"""
         |val $recordTypeInfoVal = ${liftTypeDescriptorToTypeInformation(recordType)}
         |val $dataStreamVal = com.amazon.milan.flink.runtime.DataSourceUtil.addFileDataSource[${recordType.toTerm}](
         |  ${outputs.streamEnvVal},
         |  ${source.path},
         |  ${source.dataFormat},
         |  ${source.configuration},
         |  $recordTypeInfoVal)
         |val $outputStreamVal = $dataStreamVal.map(new ${nameOf[WrapRecordsMapFunction[Any]]}[${recordType.toTerm}]($recordTypeInfoVal))
         |""".strip

    outputs.appendMain(code)

    GeneratedUnkeyedDataStream(streamIdentifier, outputStreamVal, recordType, types.EmptyTuple, isContextual = false)
  }

  private def addDelayedListDataSource(outputs: GeneratorOutputs,
                                       source: DelayedListDataSource[_],
                                       outputStreamVal: ValName,
                                       streamIdentifier: String): GeneratedUnkeyedDataStream = {
    val recordTypeInfoVal = outputs.newValName(s"${outputStreamVal}_typeinfo_")
    val valuesVal = outputs.newValName(s"${outputStreamVal}_values_")

    val sourceJson = MilanObjectMapper.writeValueAsString(source.values)
    val itemTypeName = source.getGenericArguments.head.toTerm

    val sourceFunctionVal = outputs.newValName(s"${outputStreamVal}_sourceFunction_")

    val dataStreamVal = outputs.newValName(s"${outputStreamVal}_records_")
    val recordType = source.getGenericArguments.head

    val codeBlock =
      q"""
         |val $recordTypeInfoVal = ${liftTypeDescriptorToTypeInformation(recordType)}
         |val $valuesVal = com.amazon.milan.flink.runtime.RuntimeUtil.loadJsonList[$itemTypeName]($sourceJson)
         |val $sourceFunctionVal = new ${nameOf[DelayedListSourceFunction[Any]]}[$itemTypeName]($valuesVal, ${source.runForever})
         |val $dataStreamVal = ${outputs.streamEnvVal}.addSource($sourceFunctionVal, $recordTypeInfoVal).setParallelism(1)
         |val $outputStreamVal = $dataStreamVal.map(new ${nameOf[WrapRecordsMapFunction[Any]]}[${recordType.toTerm}]($recordTypeInfoVal))
         |""".strip

    outputs.appendMain(codeBlock)

    GeneratedUnkeyedDataStream(streamIdentifier, outputStreamVal, recordType, types.EmptyTuple, isContextual = false)
  }

  private def addKinesisDataSource(outputs: GeneratorOutputs,
                                   source: KinesisDataSource[_],
                                   outputStreamVal: ValName,
                                   streamIdentifier: String): GeneratedUnkeyedDataStream = {
    val recordType = source.getGenericArguments.head
    val recordTypeInfoVal = outputs.newValName(s"${outputStreamVal}_typeinfo_")
    val dataStreamVal = outputs.newValName(s"${outputStreamVal}_records_")

    val code =
      q"""val $recordTypeInfoVal = ${liftTypeDescriptorToTypeInformation(recordType)}
         |val $dataStreamVal = com.amazon.milan.flink.runtime.KinesisDataSource.addDataSource[${recordType.toTerm}](
         |  ${outputs.streamEnvVal},
         |  ${source.streamName},
         |  ${source.region},
         |  ${source.dataFormat},
         |  $recordTypeInfoVal)
         |val $outputStreamVal = $dataStreamVal.map(new ${nameOf[WrapRecordsMapFunction[Any]]}[${recordType.toTerm}]($recordTypeInfoVal)
         |""".strip

    outputs.appendMain(code)

    GeneratedUnkeyedDataStream(streamIdentifier, outputStreamVal, recordType, types.EmptyTuple, isContextual = false)
  }
}
