package com.amazon.milan.flink.generator

import com.amazon.milan.application.DataSource
import com.amazon.milan.application.sources.{FileDataSource, ListDataSource}
import com.amazon.milan.flink.application.sources.{FlinkFileDataSource, FlinkListDataSource}
import com.amazon.milan.flink.runtime.{DelayedListSourceFunction, WrapRecordsMapFunction}
import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.testing.DelayedListDataSource
import com.amazon.milan.typeutil.types


trait DataSourceGenerator {
  val typeLifter: TypeLifter

  import typeLifter._

  def addDataSource(env: GeneratorOutputs,
                    source: DataSource[_],
                    outputStreamVal: ValName,
                    streamIdentifier: String): GeneratedUnkeyedDataStream = {
    source match {
      case listDataSource: ListDataSource[_] =>
        this.addListDataSource(env, listDataSource, outputStreamVal, streamIdentifier)

      case fileDataSource: FileDataSource[_] =>
        this.addFileDataSource(env, fileDataSource, outputStreamVal, streamIdentifier)

      case delayedListDataSource: DelayedListDataSource[_] =>
        this.addDelayedListDataSource(env, delayedListDataSource, outputStreamVal, streamIdentifier)
    }
  }

  private def addListDataSource(output: GeneratorOutputs,
                                source: ListDataSource[_],
                                outputStreamVal: ValName,
                                streamIdentifier: String): GeneratedUnkeyedDataStream = {
    val recordTypeInfoVal = output.newValName(s"${outputStreamVal}_typeinfo_")
    val valuesVal = output.newValName(s"${outputStreamVal}_values_")

    val sourceJson = ScalaObjectMapper.writeValueAsString(source.values)
    val itemTypeName = source.getGenericArguments.head.toTerm

    val dataStreamVal = output.newValName(s"${outputStreamVal}_records_")
    val recordType = source.getGenericArguments.head

    val code =
      q"""
         |val $recordTypeInfoVal = ${liftTypeDescriptorToTypeInformation(recordType)}
         |val $valuesVal = com.amazon.milan.flink.runtime.RuntimeUtil.loadJsonList[$itemTypeName]($sourceJson)
         |val $dataStreamVal = new ${nameOf[FlinkListDataSource[Any]]}[$itemTypeName]($valuesVal, ${source.runForever}, $recordTypeInfoVal).addDataSource(${output.streamEnvVal})
         |val $outputStreamVal = $dataStreamVal.map(new ${nameOf[WrapRecordsMapFunction[Any]]}[${recordType.toFlinkTerm}]($recordTypeInfoVal))
         |""".strip

    output.appendMain(code)

    GeneratedUnkeyedDataStream(streamIdentifier, outputStreamVal, recordType, types.Unit, isContextual = false)
  }

  private def addFileDataSource(output: GeneratorOutputs,
                                source: FileDataSource[_],
                                outputStreamVal: ValName,
                                streamIdentifier: String): GeneratedUnkeyedDataStream = {
    val flinkSourceVal = output.newValName(s"${outputStreamVal}_datasource_")
    val recordType = source.getGenericArguments.head

    val recordTypeInfoVal = output.newValName(s"${outputStreamVal}_typeinfo_")

    val dataStreamVal = output.newValName(s"${outputStreamVal}_records_")

    val code =
      q"""
         |val $recordTypeInfoVal = ${liftTypeDescriptorToTypeInformation(recordType)}
         |val $flinkSourceVal = new ${nameOf[FlinkFileDataSource[Any]]}[${recordType.toFlinkTerm}](${source.path}, ${source.dataFormat}, ${source.configuration}, $recordTypeInfoVal)
         |val $dataStreamVal = $flinkSourceVal.addDataSource(env)
         |val $outputStreamVal = $dataStreamVal.map(new ${nameOf[WrapRecordsMapFunction[Any]]}[${recordType.toFlinkTerm}]($recordTypeInfoVal))
         |""".strip

    output.appendMain(code)

    GeneratedUnkeyedDataStream(streamIdentifier, outputStreamVal, recordType, types.Unit, isContextual = false)
  }

  private def addDelayedListDataSource(output: GeneratorOutputs,
                                       source: DelayedListDataSource[_],
                                       outputStreamVal: ValName,
                                       streamIdentifier: String): GeneratedUnkeyedDataStream = {
    val recordTypeInfoVal = output.newValName(s"${outputStreamVal}_typeinfo_")
    val valuesVal = output.newValName(s"${outputStreamVal}_values_")

    val sourceJson = ScalaObjectMapper.writeValueAsString(source.values)
    val itemTypeName = source.getGenericArguments.head.toTerm

    val sourceFunctionVal = output.newValName(s"${outputStreamVal}_sourceFunction_")

    val dataStreamVal = output.newValName(s"${outputStreamVal}_records_")
    val recordType = source.getGenericArguments.head

    val codeBlock =
      q"""
         |val $recordTypeInfoVal = ${liftTypeDescriptorToTypeInformation(recordType)}
         |val $valuesVal = com.amazon.milan.flink.runtime.RuntimeUtil.loadJsonList[$itemTypeName]($sourceJson)
         |val $sourceFunctionVal = new ${nameOf[DelayedListSourceFunction[Any]]}[$itemTypeName]($valuesVal, ${source.runForever})
         |val $dataStreamVal = ${output.streamEnvVal}.addSource($sourceFunctionVal, $recordTypeInfoVal).setParallelism(1)
         |val $outputStreamVal = $dataStreamVal.map(new ${nameOf[WrapRecordsMapFunction[Any]]}[${recordType.toFlinkTerm}]($recordTypeInfoVal))
         |""".strip

    output.appendMain(codeBlock)

    GeneratedUnkeyedDataStream(streamIdentifier, outputStreamVal, recordType, types.Unit, isContextual = false)
  }
}
