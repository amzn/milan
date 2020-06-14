package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.compiler.scala._
import com.amazon.milan.compiler.flink.runtime.{InstantExtractorEventTimeAssigner, RecordWrapperEveryElementTrigger}
import com.amazon.milan.program.{SlidingWindow, TimeWindowExpression, TumblingWindow}
import com.amazon.milan.typeutil.{TypeDescriptor, types}
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow, Window}


trait WindowedStreamsGenerator extends KeyedStreamGenerator {
  val typeLifter: FlinkTypeLifter

  import typeLifter._

  /**
   * Applies a windowing operation to a keyed stream such that all records for a key fall into the same window.
   *
   * @param output      The generator output collector.
   * @param keyedStream The keyed stream that will have the grouping operation applied.
   * @return A [[GeneratedKeyedWindowedStream]] containing the result of applying the operation.
   */
  def applyGlobalWindowToKeyedStream(output: GeneratorOutputs,
                                     keyedStream: GeneratedKeyedDataStream): GeneratedKeyedWindowedStream = {

    val windowedStreamVal = output.newValName(s"${keyedStream.streamVal}_windowed_")
    val windowVal = output.newValName(s"${keyedStream.streamVal}_window_")
    val triggerVal = output.newValName(s"${keyedStream.streamVal}_trigger_")

    val codeBlock =
      q"""
         |val $windowVal = ${nameOf[GlobalWindows]}.create()
         |val $triggerVal = new ${nameOf[RecordWrapperEveryElementTrigger[Any, Product, Window]]}[${keyedStream.recordType.toFlinkTerm}, ${keyedStream.keyType.toTerm}, ${nameOf[GlobalWindow]}]
         |val $windowedStreamVal = ${keyedStream.streamVal}
         |  .window($windowVal)
         |  .trigger($triggerVal)
         |""".strip

    output.appendMain(codeBlock)

    val windowKeyType = KeyExtractorUtils.getWindowKeyType(keyedStream.keyType)

    GeneratedKeyedWindowedStream(
      keyedStream.streamId + "_windowed",
      windowedStreamVal,
      keyedStream.recordType,
      keyedStream.keyType,
      windowKeyType,
      isContextual = false)
  }

  /**
   * Applies a time windowing operation to a stream which has already had the correct event time applied.
   *
   * @param output      The generator output collector.
   * @param windowExpr  The expression representing the time windowing operation to apply.
   * @param inputStream The stream that will have the windowing operation applied.
   * @return A [[GeneratedGroupedStream]] containing the result of applying the operation.
   */
  def applyTimeWindowToEventTimeStream(output: GeneratorOutputs,
                                       windowExpr: TimeWindowExpression,
                                       inputStream: GeneratedStream): GeneratedGroupedStream = {
    val windowAssignerVal = output.newValName(s"stream_${windowExpr.nodeName}_windowAssigner_")
    val windowAssignerCreationStatement = this.getWindowAssignerCreationStatement(windowExpr)

    val triggerVal = output.newValName(s"stream_${windowExpr.nodeName}_trigger_")

    val outputStreamVal = output.newStreamValName(windowExpr)
    val inputRecordType = inputStream.recordType

    val createWindowStatement =
      inputStream match {
        case GeneratedUnkeyedDataStream(_, inputStreamVal, _, _, _) =>
          qc"""
              |$inputStreamVal
              |  .windowAll($windowAssignerVal)
              |  .trigger($triggerVal)
              |"""

        case GeneratedKeyedDataStream(_, inputStreamVal, _, _, _) =>
          qc"""
              |$inputStreamVal
              |  .window($windowAssignerVal)
              |  .trigger($triggerVal)
              |"""
      }

    val codeBlock =
      q"""
         |val $windowAssignerVal = $windowAssignerCreationStatement
         |val $triggerVal = new ${nameOf[RecordWrapperEveryElementTrigger[Any, Product, Window]]}[${inputRecordType.toFlinkTerm}, ${inputStream.keyType.toTerm}, ${nameOf[TimeWindow]}]()
         |val $outputStreamVal = $createWindowStatement
         |""".strip

    output.appendMain(codeBlock)

    inputStream match {
      case _: GeneratedUnkeyedDataStream =>
        GeneratedUnkeyedWindowStream(windowExpr.nodeId, outputStreamVal, inputRecordType, types.Instant, isContextual = false)

      case GeneratedKeyedDataStream(_, _, _, keyType, _) =>
        GeneratedKeyedWindowedStream(windowExpr.nodeId, outputStreamVal, inputRecordType, keyType, types.Instant, isContextual = false)
    }
  }

  def applyEventTime(env: GeneratorOutputs,
                     windowExpr: TimeWindowExpression,
                     inputStream: GeneratedDataStream): GeneratedDataStream = {
    val timeAssignerVal = env.newValName(s"stream_${windowExpr.nodeName}_timeAssigner_")
    val timeAssignerClassName = this.generateTimeAssigner(env, windowExpr, inputStream.recordType, inputStream.keyType)
    val outputStreamVal = env.newStreamValName(s"${windowExpr.nodeName}_withEventTime_")

    env.appendMain(
      q"""val $timeAssignerVal = new $timeAssignerClassName()
         |val $outputStreamVal = ${inputStream.streamVal}.assignTimestampsAndWatermarks($timeAssignerVal)
         |""".strip)

    val eventTimeStream = GeneratedUnkeyedDataStream(inputStream.streamId, outputStreamVal, inputStream.recordType, inputStream.keyType, inputStream.isContextual)

    this.keyStreamIfInputIsKeyed(env, inputStream, eventTimeStream, s"${windowExpr.nodeName}_withEventTime_final")
  }

  private def generateTimeAssigner(env: GeneratorOutputs,
                                   windowExpr: TimeWindowExpression,
                                   inputRecordType: TypeDescriptor[_],
                                   keyType: TypeDescriptor[_]): ClassName = {
    val className = env.newClassName(s"TimeAssigner_${windowExpr.nodeName}")

    // TODO: Figure out something more appropriate for the watermark logic.
    val watermarkDelay = qc"${windowExpr.size.asJava}"

    val getInstantDef = env.scalaGenerator.getScalaFunctionDef("getInstant", windowExpr.expr)
    val classDef =
      q"""class $className extends ${nameOf[InstantExtractorEventTimeAssigner[Any, Product]]}[${inputRecordType.toFlinkTerm}, ${keyType.toTerm}]($watermarkDelay) {
         |  override ${code(getInstantDef).indentTail(1)}
         |}
         |"""

    env.addClassDef(classDef)

    className
  }

  private def getWindowAssignerCreationStatement(windowExpr: TimeWindowExpression): CodeBlock = {
    windowExpr match {
      case TumblingWindow(_, _, period, offset) =>
        qc"${nameOf[TumblingEventTimeWindows]}.of(${period.toFlinkTime}, ${offset.toFlinkTime})"

      case SlidingWindow(_, _, size, slide, offset) =>
        qc"${nameOf[SlidingEventTimeWindows]}.of(${size.toFlinkTime}, ${slide.toFlinkTime}, ${offset.toFlinkTime})"
    }
  }
}
