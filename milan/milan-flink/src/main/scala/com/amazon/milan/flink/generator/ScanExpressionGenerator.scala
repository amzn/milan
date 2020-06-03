package com.amazon.milan.flink.generator

import com.amazon.milan.flink.runtime.{ArgCompareKeyedProcessFunction, ArgCompareProcessFunction}
import com.amazon.milan.program.{ArgCompareExpression, ConstantValue, FunctionDef, GreaterThan, LessThan, ScanExpression, SelectTerm, StreamArgMax, StreamArgMin, TypeChecker, ValueDef}
import com.amazon.milan.typeutil.{TypeDescriptor, types}


trait ScanExpressionGenerator extends KeyedStreamGenerator {

  import typeLifter._

  /**
   * Applies a [[ScanExpression]] operation to a stream.
   *
   * @param context     The current generator context.
   * @param scanExpr    The scan expression to apply.
   * @param inputStream Information about the input stream.
   * @return The generated data stream.
   */
  def applyScanExpression(context: GeneratorContext,
                          scanExpr: ScanExpression,
                          inputStream: GeneratedStream): GeneratedDataStream = {
    scanExpr match {
      case argCompareExpr: ArgCompareExpression =>
        this.applyArgCompareExpression(context, argCompareExpr, inputStream)
    }
  }

  private def applyArgCompareExpression(context: GeneratorContext,
                                        argCompareExpr: ArgCompareExpression,
                                        inputStream: GeneratedStream): GeneratedDataStream = {
    val processFunctionClassName = this.generateScanFunctionForArgCompare(context.output, argCompareExpr, inputStream)
    val parallelStreamVal = context.output.newStreamValName(argCompareExpr)
    val processFunctionVal = context.output.newValName(s"stream_${argCompareExpr.nodeName}_processFunction_")

    val codeBlock =
      q"""
         |val $processFunctionVal = new $processFunctionClassName
         |val $parallelStreamVal = ${inputStream.streamVal}.process($processFunctionVal)
         |""".strip
    context.output.appendMain(codeBlock)

    val parallelStream = GeneratedUnkeyedDataStream(argCompareExpr.nodeId + "_stage1", parallelStreamVal, inputStream.recordType, inputStream.keyType, isContextual = false)

    inputStream match {
      case GeneratedKeyedStream(_, _, _, _) =>
        // DataStream.process() produces an unkeyed stream, but the user wouldn't expect ArgMax to remove an existing
        // grouping from the stream. We therefore need to make the stream keyed again.
        this.keyStreamByRecordKey(context.output, parallelStream, parallelStream.streamId)

      case GeneratedStream(_, recordType, _, _) =>
        // DataStream.process() processes each parallel instance of a stream separately.
        // Scan functions are expected to operate as if the stream is not parallel, so we need to perform a reduce step.
        // Flink doesn't support reducing a non-keyed stream, so instead we key it by a constant value which eliminates
        // the parallelism, then we process it again using the same ProcessFunction as before.
        val keyFunctionDef = FunctionDef(List(ValueDef("r", recordType)), ConstantValue(1, types.Int))
        val keyedStream = this.keyStreamByFunction(context.output, parallelStream, keyFunctionDef, parallelStream.streamId)

        val reducedStreamVal = context.output.newStreamValName(s"${argCompareExpr.nodeName}_reduced")

        val keyedProcessFunctionClassName = this.generateScanFunctionForArgCompare(context.output, argCompareExpr, keyedStream)
        val keyedProcessFunctionVal = context.output.newValName(s"stream_${argCompareExpr.nodeName}_keyedProcessFunction_")

        val reduceCodeBlock =
          q"""val $keyedProcessFunctionVal = new $keyedProcessFunctionClassName
             |val $reducedStreamVal = ${keyedStream.streamVal}.process($keyedProcessFunctionVal)
             |""".strip

        context.output.appendMain(reduceCodeBlock)
        GeneratedUnkeyedDataStream(argCompareExpr.nodeId, reducedStreamVal, keyedStream.recordType, keyedStream.keyType, isContextual = false)
    }
  }

  private def generateScanFunctionForArgCompare(env: GeneratorOutputs,
                                                argCompareExpr: ArgCompareExpression,
                                                inputStream: GeneratedStream): ClassName = {
    val inputRecordType = inputStream.recordType
    val keyType = inputStream.keyType
    val argType = argCompareExpr.argExpr.tpe

    val className = env.newClassName(s"ScanFunction_${argCompareExpr.nodeName}_")

    val greaterThanDef = this.getGreatherThanFunctionDef(argCompareExpr, argType)

    val getArgImpl = env.scalaGenerator.getScalaFunctionDef("getArg", argCompareExpr.argExpr)
    val greaterThanImpl = env.scalaGenerator.getScalaFunctionDef("greaterThan", greaterThanDef)

    val baseClassName =
      inputStream match {
        case _: GeneratedUnkeyedStream =>
          qc"${nameOf[ArgCompareProcessFunction[Any, Product, Any]]}[${inputRecordType.toFlinkTerm}, ${keyType.toTerm}, ${argType.toFlinkTerm}]"

        case _: GeneratedKeyedStream =>
          qc"${nameOf[ArgCompareKeyedProcessFunction[Any, Product, Any]]}[${inputRecordType.toFlinkTerm}, ${keyType.toTerm}, ${argType.toFlinkTerm}]"
      }

    val classDef =
      q"""
         |class $className extends $baseClassName(
         |  ${liftTypeDescriptorToTypeInformation(inputRecordType)},
         |  ${liftTypeDescriptorToTypeInformation(keyType)},
         |  ${liftTypeDescriptorToTypeInformation(argType)}) {
         |
         |  override ${code(getArgImpl).indentTail(1)}
         |
         |  override ${code(greaterThanImpl).indentTail(1)}
         |}
         |""".strip

    env.addClassDef(classDef)

    className
  }

  private def getGreatherThanFunctionDef(argCompareExpr: ArgCompareExpression,
                                         argType: TypeDescriptor[_]): FunctionDef = {
    val functionDef =
      argCompareExpr match {
        case _: StreamArgMax => FunctionDef(List(ValueDef("a", argType), ValueDef("b", argType)), GreaterThan(SelectTerm("a"), SelectTerm("b")))
        case _: StreamArgMin => FunctionDef(List(ValueDef("a", argType), ValueDef("b", argType)), LessThan(SelectTerm("a"), SelectTerm("b")))
      }

    TypeChecker.typeCheck(functionDef)

    functionDef
  }
}
