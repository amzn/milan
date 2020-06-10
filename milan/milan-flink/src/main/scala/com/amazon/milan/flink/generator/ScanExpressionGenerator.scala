package com.amazon.milan.flink.generator

import com.amazon.milan.flink.runtime.{ScanOperationKeyedProcessFunction, ScanOperationProcessFunction}
import com.amazon.milan.program.ScanExpression


trait ScanExpressionGenerator
  extends KeyedStreamGenerator
    with ScanOperationGenerator {

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
    inputStream match {
      case dataStream: GeneratedDataStream =>
        this.applyScanExpression(context, scanExpr, dataStream)
    }
  }

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
                          inputStream: GeneratedDataStream): GeneratedDataStream = {
    val scanOperation = this.generateScanOperation(context, scanExpr, inputStream.recordType)
    val processFunction = this.generateScanOperationProcessFunction(context.output, scanOperation, inputStream)

    val processFunctionVal = context.output.newValName(s"stream_${scanExpr.nodeName}_processFunction_")
    val outputStreamVal = context.output.newStreamValName(scanExpr.nodeName + "_unkeyed")

    val codeBlock =
      q"""val $processFunctionVal = new ${processFunction.className}()
         |val $outputStreamVal = ${inputStream.streamVal}.process($processFunctionVal, $processFunctionVal.getProducedType)
         |""".strip

    context.output.appendMain(codeBlock)

    val outputStream = GeneratedUnkeyedDataStream(
      scanExpr.nodeName + "_unkeyed",
      outputStreamVal,
      processFunction.outputRecordType,
      processFunction.outputKeyType,
      isContextual = false)

    this.keyStreamIfInputIsKeyed(context.output, inputStream, outputStream, scanExpr.nodeName)
  }

  private def generateScanOperationProcessFunction(outputs: GeneratorOutputs,
                                                   scanOperation: ScanOperationClassInfo,
                                                   inputStream: GeneratedStream): OperatorInfo = {
    val className = outputs.newClassName(s"ProcessFunction_${inputStream.streamId}_ScanOperation")

    val inputRecordType = inputStream.recordType
    val keyType = inputStream.keyType
    val stateType = scanOperation.stateType
    val outputRecordType = scanOperation.outputType

    val baseClassName =
      inputStream match {
        case _: GeneratedKeyedStream =>
          nameOf[ScanOperationKeyedProcessFunction[Any, Product, Any, Any]]

        case _: GeneratedUnkeyedStream =>
          nameOf[ScanOperationProcessFunction[Any, Product, Any, Any]]
      }

    val classDef =
      q"""class $className
         |  extends $baseClassName[${inputRecordType.toFlinkTerm}, ${keyType.toTerm}, ${stateType.toTerm}, ${outputRecordType.toFlinkTerm}](
         |  new ${scanOperation.className}(),
         |  ${liftTypeDescriptorToTypeInformation(keyType)})
         |""".stripMargin

    outputs.addClassDef(classDef)

    OperatorInfo(className, outputRecordType, keyType)
  }
}
