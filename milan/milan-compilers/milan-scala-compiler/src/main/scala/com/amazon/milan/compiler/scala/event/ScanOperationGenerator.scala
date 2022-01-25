package com.amazon.milan.compiler.scala.event

import com.amazon.milan.compiler.scala._
import com.amazon.milan.compiler.scala.event.operators._
import com.amazon.milan.lang.StateIdentifier
import com.amazon.milan.program.{AggregateExpression, ArgCompareExpression, ArgScanExpression, Count, First, FunctionDef, Max, Min, Scan, ScanExpression, StreamArgMax, StreamArgMin, StreamExpression, Sum, SumBy}
import com.amazon.milan.typeutil.{TypeDescriptor, types}


case class ScanOperationInlineClassInfo(classDef: CodeBlock,
                                        classType: CodeBlock,
                                        stateType: TypeDescriptor[_],
                                        outputType: TypeDescriptor[_])


case class ScanOperationClassInfo(className: ClassName,
                                  classDef: CodeBlock,
                                  classType: CodeBlock,
                                  stateType: TypeDescriptor[_],
                                  outputType: TypeDescriptor[_])


case class ScanOperationFieldInfo(field: ValName, outputType: TypeDescriptor[_])


/**
 * Generates code the implements scan expression.
 */
trait ScanOperationGenerator
  extends StateInterfaceGenerator
    with ConsumerGenerator
    with KeyOperationsGenerator {

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
   * Generates a code that instantiates a [[ScanOperation]] implementation for an [[AggregateExpression]].
   *
   * @param aggregateExpression  An [[AggregateExpression]] to implement with a ScanOperation.
   * @param inputMappingFunction A function definition that maps input records to the input to the aggregate expression.
   * @return A [[ScanOperationInlineClassInfo]] that contains code that instantiates the appropriate [[ScanOperation]]
   *         for the expression.
   */
  def generateScanOperationForAggregateExpression(keyType: TypeDescriptor[_],
                                                  aggregateExpression: AggregateExpression,
                                                  inputMappingFunction: FunctionDef): ScanOperationInlineClassInfo = {
    val inputType = inputMappingFunction.arguments.head.tpe
    val valueType = inputMappingFunction.tpe

    val functionGenerator = new ScalarFunctionGenerator(this.typeLifter.typeEmitter, new IdentityTreeTransformer)

    val inputMappingFunctionDef = CodeBlock(functionGenerator.getScalaFunctionDef("getValue", inputMappingFunction))

    val classType = aggregateExpression match {
      case _: Sum =>
        qc"""${nameOf[SumScanOperation[Any, Any, Int]]}[${inputType.toTerm}, ${keyType.toTerm}, ${valueType.toTerm}]"""

      case _: Min =>
        qc"""${nameOf[MinScanOperation[Any, Any, Int]]}[${inputType.toTerm}, ${keyType.toTerm}, ${valueType.toTerm}]"""

      case _: Max =>
        qc"""${nameOf[MaxScanOperation[Any, Any, Int]]}[${inputType.toTerm}, ${keyType.toTerm}, ${valueType.toTerm}]"""

      case _: First =>
        qc"""${nameOf[FirstScanOperation[Any, Any, Any]]}[${inputType.toTerm}, ${keyType.toTerm}, ${valueType.toTerm}]"""

      case _: Count =>
        qc"""${nameOf[CountScanOperation[Any, Any]]}[${inputType.toTerm}, ${keyType.toTerm}]"""
    }

    val classDef = aggregateExpression match {
      case _: Count =>
        classType

      case _ =>
        qc"""
            |$classType($valueType) {
            |  ${inputMappingFunctionDef.indentTail(1)}
            |}
            |"""
    }

    val stateType = aggregateExpression match {
      case _: Sum => valueType
      case _: Count => types.Long
      case _ => valueType.toOption
    }

    val outputType = aggregateExpression match {
      case _: Count => types.Long
      case _ => valueType
    }

    ScanOperationInlineClassInfo(classDef, classType, stateType, outputType)
  }

  /**
   * Generates a ScanOperation class that contains one or more scan operations and processes their outputs.
   *
   * @param outputs           The output collector.
   * @param inputStream       The input stream to the scan operation.
   * @param scanOperations    The inner scan operations.
   * @param outputFunctionDef A function definition that takes a tuple of the outputs from the inner scan operations
   *                          and returns an output value.
   * @return A [[ScanOperationClassInfo]] containing the generated class.
   */
  def generateCombinedScanOperations(outputs: GeneratorOutputs,
                                     inputStream: StreamInfo,
                                     scanOperations: List[ScanOperationInlineClassInfo],
                                     outputFunctionDef: FunctionDef): ScanOperationInlineClassInfo = {
    val finalOutputType = outputFunctionDef.tpe

    val operationValNames = List.tabulate(scanOperations.length)(i => qv"op${i + 1}")

    val inputType = inputStream.recordType
    val keyType = inputStream.keyType

    val stateType = TypeDescriptor.createTuple(scanOperations.map(_.stateType))

    val baseClass = qc"""${nameOf[ScanOperation[Any, Any, Any, Any]]}[${inputType.toTerm}, ${keyType.toTerm}, ${stateType.toTerm}, ${finalOutputType.toTerm}]"""

    val initialOperationStates = operationValNames.map(opName => qc"$opName.initialState")
    val initialStateDefinition = qc"Tuple${initialOperationStates.length}(..$initialOperationStates)"

    val operationStateVals = operationValNames.map(opName => qv"${opName}_state")
    val operationOutputVals = operationValNames.map(opName => qv"${opName}_output")
    val operationNewStateVals = operationValNames.map(opName => qv"${opName}_newState")

    val operationProcessCalls = operationValNames.zip(operationStateVals).zip(operationOutputVals).zip(operationNewStateVals).map {
      case (((opName, opStateVal), opOutputVal), opNewStateVal) =>
        qc"""val ($opNewStateVal, $opOutputVal) = $opName.process($opStateVal, input, key)"""
    }

    val functionGenerator = new ScalarFunctionGenerator(this.typeLifter.typeEmitter, new IdentityTreeTransformer)

    val outputFunctionDefCode = CodeBlock(functionGenerator.getScalaFunctionDef("getOutput", outputFunctionDef))

    val operationFieldVals = operationValNames.zip(scanOperations).map {
      case (valName, op) => qc"""private val $valName = new ${op.classDef}"""
    }

    val keyVal = ValName("key")
    val outputTupleVal = ValName("outputTuple")

    val getOutputArgs = if (outputFunctionDef.arguments.length == 1) List(outputTupleVal) else List(keyVal, outputTupleVal)

    val classDef =
      qc"""
          |$baseClass {
          |  //$operationFieldVals
          |
          |  override val initialState: ${stateType.toTerm} = $initialStateDefinition
          |
          |  override val stateTypeDescriptor: ${nameOf[TypeDescriptor[Any]]}[${stateType.toTerm}] = $stateType
          |
          |  override val outputTypeDescriptor: ${nameOf[TypeDescriptor[Any]]}[${finalOutputType.toTerm}] = $finalOutputType
          |
          |  private ${outputFunctionDefCode.indentTail(1)}
          |
          |  override def process(state: ${stateType.toTerm}, input: ${inputType.toTerm}, $keyVal: ${keyType.toTerm}): (${stateType.toTerm}, ${finalOutputType.toTerm}) = {
          |    val Tuple${operationStateVals.length}(..$operationStateVals) = state
          |
          |    //$operationProcessCalls
          |
          |    val $outputTupleVal = Tuple${operationOutputVals.length}(..$operationOutputVals)
          |    val newState = Tuple${operationNewStateVals.length}(..$operationNewStateVals)
          |
          |    val output = this.getOutput(..$getOutputArgs)
          |    (newState, output)
          |  }
          |}
          |"""

    ScanOperationInlineClassInfo(classDef, baseClass, stateType, finalOutputType)
  }

  /**
   * Generates a statement that creates a class that implements a [[ScanExpression]].
   *
   * @param outputs     The generator output collector.
   * @param inputStream Information about the input stream.
   * @param scanExpr    A scan expression to generate.
   * @return A [[ValName]] referencing the class field containing the generated [[ScanOperation]].
   */
  def generateScanOperationClass(outputs: GeneratorOutputs,
                                 inputStream: StreamInfo,
                                 scanExpr: ScanExpression): ScanOperationInlineClassInfo = {
    scanExpr match {
      case scanExpr: Scan =>
        this.generateScanOperation(outputs, inputStream, scanExpr)

      case argCompareExpr: ArgCompareExpression =>
        this.generateArgCompareScanOperation(outputs, inputStream, argCompareExpr)

      case argScanExpr: ArgScanExpression =>
        this.generateAssociativeScanOperation(outputs, inputStream, argScanExpr)
    }
  }

  /**
   * Generates a [[ScanOperationHost]] class that contains a scan operation, and stores it in a class field in the
   * generated output.
   *
   * @param context       The generator context.
   * @param inputStream   The input stream to the scan operation.
   * @param streamExpr    The stream expression defining the operation.
   * @param scanOperation The scan operation to host.
   * @return A [[ValName]] containing the name of the class field to which the scan operation host was assigned.
   */
  def generateScanOperationHost(context: GeneratorContext,
                                inputStream: StreamInfo,
                                streamExpr: StreamExpression,
                                scanOperation: ScanOperationInlineClassInfo): ValName = {
    val inputRecordType = inputStream.recordType
    val fullKeyType = inputStream.fullKeyType
    val keyType = inputStream.keyType
    val stateType = scanOperation.stateType
    val outputRecordType = scanOperation.outputType
    val stateStore = this.generateKeyedStateInterface(context, streamExpr, StateIdentifier.STREAM_STATE, fullKeyType, stateType)

    val fieldName = ValName(toValidName(s"scanOperationHost_${streamExpr.nodeName}"))
    val fieldType = qc"${nameOf[ScanOperationHost[Any, Any, Any, Any, Any]]}[${inputRecordType.toTerm}, ${fullKeyType.toTerm}, ${keyType.toTerm}, ${stateType.toTerm}, ${outputRecordType.toTerm}]"

    val fullKeyValName = ValName("fullKey")
    val getLocalKeyCode = this.generateGetLocalKeyCode(fullKeyValName, fullKeyType, keyType)

    val fieldDef =
      qc"""private val $fieldName: $fieldType =
          |  new $fieldType {
          |    protected override val scanOperation: ${scanOperation.classType} =
          |      new ${scanOperation.classDef.indentTail(2)}
          |
          |    protected override val state = ${stateStore.indentTail(1)}
          |
          |    protected override def getLocalKey($fullKeyValName: ${fullKeyType.toTerm}): ${keyType.toTerm} = {
          |      ${getLocalKeyCode.indentTail(3)}
          |    }
          |}
          |"""

    context.outputs.addField(fieldDef.value)

    fieldName
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
    this.generateScanOperationHost(context, inputStream, scanExpr, scanOperation)
  }

  /**
   * Generates a [[ScanOperation]] implementation for a [[Scan]] expression.
   */
  private def generateScanOperation(outputs: GeneratorOutputs,
                                    inputStream: StreamInfo,
                                    scanExpr: Scan): ScanOperationInlineClassInfo = {
    val stateType = scanExpr.initialState.tpe
    val inputType = inputStream.recordType
    val keyType = inputStream.keyType
    val outputType = scanExpr.step.tpe.genericArguments(1)

    val classType = qc"${nameOf[ScanOperation[Any, Any, Any, Any]]}[${inputType.toTerm}, ${keyType.toTerm}, ${stateType.toTerm}, ${outputType.toTerm}]"

    val stepFunctionDef = CodeBlock(outputs.scalaGenerator.getScalaFunctionDef("step", scanExpr.step))

    val classDef =
      qc"""
          |$classType {
          |  override val initialState: ${stateType.toTerm} = ${scanExpr.initialState}
          |
          |  override val stateTypeDescriptor: ${nameOf[TypeDescriptor[Any]]}[${stateType.toTerm}] = $stateType
          |
          |  override val outputTypeDescriptor: ${nameOf[TypeDescriptor[Any]]}[${outputType.toTerm}] = $outputType
          |
          |  private ${stepFunctionDef.indentTail(2)}
          |
          |  override def process(state: ${stateType.toTerm}, input: ${inputType.toTerm}, key: ${keyType.toTerm}): (${stateType.toTerm}, ${outputType.toTerm}) = {
          |    this.step(state, input)
          |  }
          |}
          |"""

    ScanOperationInlineClassInfo(classDef, classType, stateType, outputType)
  }

  /**
   * Generates a [[ScanOperation]] class that implemented an [[ArgCompareExpression]].
   */
  private def generateArgCompareScanOperation(outputs: GeneratorOutputs,
                                              inputStream: StreamInfo,
                                              argCompareExpr: ArgCompareExpression): ScanOperationInlineClassInfo = {

    val argType = argCompareExpr.argExpr.tpe

    val greaterThanBody =
      argCompareExpr match {
        case _: StreamArgMax => qc"ordering.gt(arg1, arg2)"
        case _: StreamArgMin => qc"ordering.lt(arg1, arg2)"
      }

    val getArgDef = outputs.scalaGenerator.getScalaFunctionDef("getArg", argCompareExpr.argExpr)
    val recordType = inputStream.recordType

    val classType = qc"${nameOf[ArgMaxScanOperation[Any, Any, Any]]}[${recordType.toTerm}, ${inputStream.keyType.toTerm}, ${argType.toTerm}]"

    val classDef =
      qc"""$classType(
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

    val stateType = TypeDescriptor.optionOf(TypeDescriptor.createTuple(List(argType, recordType)))

    ScanOperationInlineClassInfo(classDef, classType, stateType, recordType)
  }

  /**
   * Generates a [[ScanOperation]] class for an [[ArgScanExpression]].
   */
  private def generateAssociativeScanOperation(output: GeneratorOutputs,
                                               inputStream: StreamInfo,
                                               argScanExpr: ArgScanExpression): ScanOperationInlineClassInfo = {
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
    val classType = qc"${nameOf[AssociativeScanOperation[Any, Any, Any, Any]]}[${inputRecordType.toTerm}, ${inputStream.keyType.toTerm}, ${argType.toTerm}, ${outputRecordType.toTerm}]"

    val classDef =
      qc"""$classType(
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

    ScanOperationInlineClassInfo(classDef, classType, argType, outputRecordType)
  }

  private def getAssociativeScanOperationInitialValue(argScanExpr: ArgScanExpression, numericField: ValName): CodeBlock = {
    argScanExpr match {
      case _: SumBy => qc"this.$numericField.zero"
    }
  }
}
