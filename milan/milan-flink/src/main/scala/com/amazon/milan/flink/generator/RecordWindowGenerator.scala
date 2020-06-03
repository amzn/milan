package com.amazon.milan.flink.generator

import com.amazon.milan.flink.runtime.{RecordWindowApplyAggregateFunction, RecordWrapperEveryElementTrigger}
import com.amazon.milan.program.{SlidingRecordWindow, StreamMap}
import com.amazon.milan.typeutil.types
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, Window}


trait RecordWindowGenerator
  extends FunctionGenerator
    with ProcessWindowFunctionGenerator {

  val typeLifter: TypeLifter

  import typeLifter._

  def applyRecordWindowApplyOfKeyedStream(context: GeneratorContext,
                                          inputStream: GeneratedKeyedDataStream,
                                          applyExpr: StreamMap,
                                          windowExpr: SlidingRecordWindow): GeneratedUnkeyedDataStream = {
    val aggregateFunction = this.generateRecordWindowApplyAggregateFunction(
      context.output,
      inputStream,
      applyExpr,
      windowExpr)

    val processAllWindowFunctionClassName = this.generateAssignSequenceNumberProcessAllWindowFunction(
      context.output,
      aggregateFunction.outputRecordType,
      aggregateFunction.outputKeyType,
      applyExpr.nodeName)

    val aggregateFunctionVal = context.output.newValName(s"stream_${applyExpr.nodeName}_aggregateFunction_")
    val processWindowVal = context.output.newValName(s"stream_${applyExpr.nodeName}_processAllWindowFunction_")
    val triggerVal = context.output.newValName(s"stream_${applyExpr.nodeName}_trigger_")
    val outputStreamVal = context.output.newStreamValName(applyExpr)

    val codeBlock =
      q"""val $aggregateFunctionVal = new ${aggregateFunction.className}()
         |val $processWindowVal = new $processAllWindowFunctionClassName()
         |val $triggerVal = new ${nameOf[RecordWrapperEveryElementTrigger[Any, Product, Window]]}[${inputStream.recordType.toFlinkTerm}, ${inputStream.keyType.toTerm}, ${nameOf[GlobalWindow]}]
         |val $outputStreamVal = ${inputStream.streamVal}
         |  .windowAll(${nameOf[GlobalWindows]}.create())
         |  .trigger($triggerVal)
         |  .aggregate(
         |    $aggregateFunctionVal,
         |    $processWindowVal,
         |    $aggregateFunctionVal.getAccumulatorType,
         |    $aggregateFunctionVal.getProducedType,
         |    $aggregateFunctionVal.getProducedType)
         |""".strip

    context.output.appendMain(codeBlock)

    GeneratedUnkeyedDataStream(applyExpr.nodeId, outputStreamVal, applyExpr.recordType.toFlinkRecordType, types.Unit, isContextual = false)
  }


  private def generateRecordWindowApplyAggregateFunction(output: GeneratorOutputs,
                                                         inputStream: GeneratedKeyedDataStream,
                                                         applyExpr: StreamMap,
                                                         windowExpr: SlidingRecordWindow): OperatorInfo = {
    val outputRecordType = applyExpr.expr.tpe.toFlinkRecordType
    val applyDef = this.getFunctionDefinition(output, "apply", applyExpr.expr, outputRecordType)

    val inputRecordType = inputStream.recordType
    val recordKeyType = inputStream.keyType

    val getKeyFunction = KeyExtractorUtils.getRecordKeyToKeyFunction(inputStream, applyExpr)
    val getKeyDef = output.scalaGenerator.getScalaFunctionDef("getKey", getKeyFunction)
    val groupKeyType = getKeyFunction.tpe

    val className = output.newClassName(s"AggregateFunction_${applyExpr.nodeName}_")

    val classDef =
      q"""class $className
         |  extends ${nameOf[RecordWindowApplyAggregateFunction[Any, Product, Any, Any]]}[${inputRecordType.toFlinkTerm}, ${recordKeyType.toTerm}, ${groupKeyType.toTerm}, ${outputRecordType.toFlinkTerm}](
         |  ${windowExpr.windowSize},
         |  ${liftTypeDescriptorToTypeInformation(inputRecordType)},
         |  ${liftTypeDescriptorToTypeInformation(recordKeyType)},
         |  ${liftTypeDescriptorToTypeInformation(groupKeyType)},
         |  ${liftTypeDescriptorToTypeInformation(outputRecordType)}) {
         |
         |  override ${code(getKeyDef).indentTail(1)}
         |
         |  override ${applyDef.indentTail(1)}
         |}
         |""".strip

    output.addClassDef(classDef)

    OperatorInfo(className, outputRecordType, types.Unit)
  }
}
