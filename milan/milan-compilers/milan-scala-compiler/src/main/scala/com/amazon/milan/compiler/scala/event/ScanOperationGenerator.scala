package com.amazon.milan.compiler.scala.event

import com.amazon.milan.compiler.scala._
import com.amazon.milan.compiler.scala.event.operators.{ArgMaxScanOperation, AssociativeScanOperation, ScanOperation, ScanOperationHost}
import com.amazon.milan.lang.StateIdentifier
import com.amazon.milan.program.{ArgCompareExpression, ArgScanExpression, ScanExpression, StreamArgMax, StreamArgMin, SumBy}
import com.amazon.milan.typeutil.TypeDescriptor


case class ScanOperationClassInfo(classDef: CodeBlock, classType: CodeBlock, stateType: TypeDescriptor[_], outputType: TypeDescriptor[_])


case class ScanOperationFieldInfo(field: ValName, outputType: TypeDescriptor[_])


/**
 * Generates code the implements scan expression.
 */
trait ScanOperationGenerator
  extends StateInterfaceGenerator
    with ConsumerGenerator {

  val typeLifter: TypeLifter

  import typeLifter._

  /**
   * Generates the consumer method for a scan operation.
   *
   * @param context     The generator context.
   * @param inputStream The input stream to the scan operation.
   * @param scanExpr    The scan expression defining the operation.
   * @return A [[StreamInfo]] containing information about the generated operation.
   */
  def generateScan(context: GeneratorContext,
                   inputStream: StreamInfo,
                   scanExpr: ScanExpression): StreamInfo = {
    val scanOperationHostField = this.generateScanOperationHost(context, inputStream, scanExpr)

    val recordArg = ValName("record")
    val collector = context.outputs.getCollectorName(scanExpr)

    val methodBody =
      qc"""this.$scanOperationHostField.processRecord($recordArg) match {
          |  case Some(value) => $collector(value)
          |  case None => ()
          |}
          |"""

    val consumerInfo = StreamConsumerInfo(scanExpr.nodeName, "input")
    this.generateConsumer(context.outputs, inputStream, consumerInfo, recordArg, methodBody)

    inputStream.withExpression(scanExpr)
  }

  /**
   * Generates a [[ScanOperationHost]] class that implements a scan expression and stores it in a class field in
   * the generated output.
   *
   * @param context     The generator context.
   * @param inputStream The input stream to the scan operation.
   * @param scanExpr    The scan expression defining the operation.
   * @return A [[ValName]] containing the name of the class field to which the scan operation host was assigned.
   */
  private def generateScanOperationHost(context: GeneratorContext,
                                        inputStream: StreamInfo,
                                        scanExpr: ScanExpression): ValName = {
    val scanOperation = this.generateScanOperationClass(context.outputs, inputStream, scanExpr)

    val inputRecordType = inputStream.recordType
    val keyType = inputStream.fullKeyType
    val stateType = scanOperation.stateType
    val outputRecordType = scanOperation.outputType
    val stateStore = this.generateKeyedStateInterface(context, scanExpr, StateIdentifier.STREAM_STATE, keyType, stateType)

    val fieldName = ValName(context.outputs.cleanName(s"scanOperationHost_${scanExpr.nodeName}"))
    val fieldType = qc"${nameOf[ScanOperationHost[Any, Any, Any, Any]]}[${inputRecordType.toTerm}, ${keyType.toTerm}, ${stateType.toTerm}, ${outputRecordType.toTerm}]"

    val fieldDef =
      qc"""private val $fieldName: $fieldType =
          |  new $fieldType {
          |    protected override val scanOperation: ${scanOperation.classType} =
          |      ${scanOperation.classDef.indentTail(2)}
          |
          |    protected override val state = ${stateStore.indentTail(1)}
          |}
          |"""

    context.outputs.addField(fieldDef.value)

    fieldName
  }

  /**
   * Generates a class field containing a [[ScanOperation]] that implements a [[ScanExpression]].
   *
   * @param outputs     The generator output collector.
   * @param inputStream Information about the input stream.
   * @param scanExpr    A scan expression to generate.
   * @return A [[ValName]] referencing the class field containing the generated [[ScanOperation]].
   */
  private def generateScanOperationClass(outputs: GeneratorOutputs,
                                         inputStream: StreamInfo,
                                         scanExpr: ScanExpression): ScanOperationClassInfo = {
    scanExpr match {
      case argCompareExpr: ArgCompareExpression =>
        this.generateArgCompareScanOperation(outputs, inputStream, argCompareExpr)

      case argScanExpr: ArgScanExpression =>
        this.generateAssociativeScanOperation(outputs, inputStream, argScanExpr)
    }
  }

  private def generateArgCompareScanOperation(outputs: GeneratorOutputs,
                                              inputStream: StreamInfo,
                                              argCompareExpr: ArgCompareExpression): ScanOperationClassInfo = {

    val argType = argCompareExpr.argExpr.tpe

    val greaterThanBody =
      argCompareExpr match {
        case _: StreamArgMax => qc"ordering.gt(arg1, arg2)"
        case _: StreamArgMin => qc"ordering.lt(arg1, arg2)"
      }

    val getArgDef = outputs.scalaGenerator.getScalaFunctionDef("getArg", argCompareExpr.argExpr)
    val recordType = inputStream.recordType

    val classType = qc"${nameOf[ArgMaxScanOperation[Any, Any]]}[${recordType.toTerm}, ${argType.toTerm}]"

    val classDef =
      qc"""new $classType(
          |    $recordType,
          |    $argType) {
          |
          |    override def greaterThan(ordering: Ordering[${argType.toTerm}], arg1: ${argType.toTerm}, arg2: ${argType.toTerm}): Boolean = {
          |      ${greaterThanBody.indentTail(3)}
          |    }
          |
          |    override ${code(getArgDef).indentTail(2)}
          |}
          |"""

    ScanOperationClassInfo(classDef, classType, TypeDescriptor.optionOf(argType), recordType)
  }

  /**
   * Generates a [[ScanOperation]] class for an [[ArgScanExpression]].
   */
  private def generateAssociativeScanOperation(output: GeneratorOutputs,
                                               inputStream: StreamInfo,
                                               argScanExpr: ArgScanExpression): ScanOperationClassInfo = {
    val inputRecordType = inputStream.recordType
    val argType = argScanExpr.argExpr.tpe
    val outputRecordType = argScanExpr.outputExpr.tpe

    val getArgDef = output.scalaGenerator.getScalaFunctionDef("getArg", argScanExpr.argExpr)

    val getOutputDef = output.scalaGenerator.getScalaFunctionDef("getOutput", argScanExpr.outputExpr)

    val numericField = ValName("argNumeric")

    val addDef =
      argScanExpr match {
        case _: SumBy => qc"this.$numericField.plus(arg1, arg2)"
      }

    val initialState = this.getAssociativeScanOperationInitialValue(argScanExpr, numericField)
    val classType = qc"${nameOf[AssociativeScanOperation[Any, Any, Any]]}[${inputRecordType.toTerm}, ${argType.toTerm}, ${outputRecordType.toTerm}]"

    val classDef =
      qc"""new $classType(
          |  $argType,
          |  $outputRecordType) {
          |
          |  override val initialState: ${argType.toTerm} = $initialState
          |
          |  override ${code(getArgDef).indentTail(1)}
          |
          |  override ${code(getOutputDef).indentTail(1)}
          |
          |  override def add(numeric: Numeric[${argType.toTerm}], arg1: ${argType.toTerm}, arg2: ${argType.toTerm}): ${argType.toTerm} = {
          |    $addDef
          |  }
          |
          |}
          |"""

    ScanOperationClassInfo(classDef, classType, argType, outputRecordType)
  }

  private def getAssociativeScanOperationInitialValue(argScanExpr: ArgScanExpression, numericField: ValName): CodeBlock = {
    argScanExpr match {
      case _: SumBy => qc"this.$numericField.zero"
    }
  }
}
