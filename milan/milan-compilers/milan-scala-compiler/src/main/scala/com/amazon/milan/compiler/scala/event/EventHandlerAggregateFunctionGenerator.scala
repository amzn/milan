package com.amazon.milan.compiler.scala.event

import com.amazon.milan.compiler.scala._
import com.amazon.milan.compiler.scala.event.operators.{FoldScanOperation, KeyedSlidingRecordWindowApply}
import com.amazon.milan.compiler.scala.trees.AggregateFunctionTreeExtractor
import com.amazon.milan.lang.StateIdentifier
import com.amazon.milan.program.{Aggregate, SlidingRecordWindow, TypeChecker}
import com.amazon.milan.typeutil.{TypeDescriptor, types}

import scala.language.existentials


trait EventHandlerAggregateFunctionGenerator
  extends ScanOperationGenerator
    with StateInterfaceGenerator {

  val typeLifter: TypeLifter

  import typeLifter._

  /**
   * Generates the implementation of an [[Aggregate]] expression where the input is a data stream.
   */
  def generateAggregateOfDataStream(context: GeneratorContext,
                                    inputStream: StreamInfo,
                                    aggregateExpr: Aggregate): StreamInfo = {
    val scanOperation = this.generateScanOperationForAggregateWithKey(context, inputStream, aggregateExpr)
    val scanOperationHost = this.generateScanOperationHost(context, inputStream, aggregateExpr, scanOperation)

    val collector = context.outputs.getCollectorName(aggregateExpr)

    val recordArg = ValName("record")
    val methodBody =
      qc"""
          | val output = $scanOperationHost.processRecord($recordArg)
          | output match {
          |   case Some(value) => $collector(value)
          |   case None => ()
          | }
        """

    val consumerInfo = StreamConsumerInfo(aggregateExpr.nodeName, "input")

    this.generateConsumer(context.outputs, inputStream, consumerInfo, recordArg, methodBody)

    inputStream.withExpression(aggregateExpr)
  }

  /**
   * Generates the implementation of an [[Aggregate]] expression where the input is a grouped windowed stream.
   */
  def generateAggregateOfGroupedRecordWindow(context: GeneratorContext,
                                             inputStream: StreamInfo,
                                             aggregateExpr: Aggregate,
                                             sourceWindow: SlidingRecordWindow): StreamInfo = {
    val applyClassDef = this.generateKeyedSlidingWindowApplyClass(context, inputStream, aggregateExpr, sourceWindow)
    val applyField = ValName(context.outputs.newFieldName(s"slidingRecordWindowApply_${aggregateExpr.nodeName}_"))
    val applyFieldDef = qc"private val $applyField = new ${applyClassDef.indentTail(1)}"

    val collector = context.outputs.getCollectorName(aggregateExpr)

    context.outputs.addField(applyFieldDef.value)

    val recordArg = ValName("record")
    val methodBody =
      qc"""
          |val output = $applyField.processRecord($recordArg)
          |$collector(output)
          |"""

    val consumerInfo = StreamConsumerInfo(aggregateExpr.nodeName, "input")

    this.generateConsumer(context.outputs, inputStream, consumerInfo, recordArg, methodBody)

    inputStream.withExpression(aggregateExpr).withKeyType(types.EmptyTuple)
  }

  private def generateKeyedSlidingWindowApplyClass(context: GeneratorContext,
                                                   inputStream: StreamInfo,
                                                   aggregateExpr: Aggregate,
                                                   windowExpr: SlidingRecordWindow): CodeBlock = {
    val scanOperation = this.generateScanOperationForAggregateWithoutKey(context, inputStream, aggregateExpr)

    val inputType = inputStream.recordType
    val fullKeyType = inputStream.fullKeyType
    val contextKeyType = inputStream.contextKeyType
    val keyType = inputStream.keyType
    val stateType = scanOperation.stateType
    val outputType = aggregateExpr.recordType

    val scanOperationVal = ValName("scanOperation")

    val windowSize = windowExpr.windowSize

    val foldOperationInstanceDef =
      qc"new ${nameOf[FoldScanOperation[Any, Any, Any, Any]]}[${inputType.toTerm}, ${keyType.toTerm}, ${stateType.toTerm}, ${outputType.toTerm}]($scanOperationVal)"

    val fullKeyVal = ValName("fullKey")
    val getLocalKeyCode = this.generateGetLocalKeyCode(fullKeyVal, fullKeyType, keyType)
    val getOutputKeyCode = this.generateGetContextKeyCode(fullKeyVal, fullKeyType, contextKeyType)

    val (stateKeyType, getStateKeyCode) =
      if (contextKeyType == types.EmptyTuple) {
        // There is no context key, but we still need a key for the keyed state interface, so use a constant integer.
        (types.Int, qc"0")
      }
      else {
        // There is a context key, which means we can use the same code we used to get the output key, which is just the
        // context key.
        (contextKeyType, getOutputKeyCode)
      }

    val windowStateType = TypeDescriptor.ofMap(keyType, TypeDescriptor.ofList(inputType))
    val windowStateDef = this.generateKeyedStateInterface(context, aggregateExpr, StateIdentifier.STREAM_STATE, stateKeyType, windowStateType)

    qc"""
        |${nameOf[KeyedSlidingRecordWindowApply[Any, Any, Any, Any, Any, Any]]}[${inputType.toTerm}, ${fullKeyType.toTerm}, ${stateKeyType.toTerm}, ${keyType.toTerm}, ${outputType.toTerm}, ${contextKeyType.toTerm}]($windowSize) {
        |  private val scanOperation = new ${scanOperation.classDef.indentTail(1)}
        |
        |  private val foldOperation = $foldOperationInstanceDef
        |
        |  protected val windowState: ${nameOf[KeyedStateInterface[Any, Any]]}[${stateKeyType.toTerm}, Map[${keyType.toTerm}, List[${inputType.toTerm}]]] =
        |    $windowStateDef
        |
        |  protected override def getLocalKey($fullKeyVal: ${fullKeyType.toTerm}): ${keyType.toTerm} = {
        |    ${getLocalKeyCode.indentTail(2)}
        |  }
        |
        |  protected override def getStateKey($fullKeyVal: ${fullKeyType.toTerm}): ${stateKeyType.toTerm} = {
        |    ${getStateKeyCode.indentTail(2)}
        |  }
        |
        |  protected override def getOutputKey($fullKeyVal: ${fullKeyType.toTerm}): ${contextKeyType.toTerm} = {
        |    ${getOutputKeyCode.indentTail(2)}
        |  }
        |
        |  protected override def applyWindow(items: Iterable[${inputType.toTerm}], key: ${keyType.toTerm}): ${outputType.toTerm} =
        |    this.foldOperation.fold(items, key)
        |}
        |"""
  }

  private def generateScanOperationForAggregateWithoutKey(context: GeneratorContext,
                                                          inputStream: StreamInfo,
                                                          aggregateExpr: Aggregate): ScanOperationInlineClassInfo = {
    // The map function will be made up of zero or more function calls to aggregate functions, combined together
    // in some expression tree.
    // We need to separate out those aggregate function calls and create a flink AggregateFunction for each one.
    // Each of these separate AggregateFunctions will have the same input type, which is the input stream record type.
    val aggregateExpressions = AggregateFunctionTreeExtractor.getAggregateExpressions(aggregateExpr.expr)
    val aggregateExpressionInputMaps = AggregateFunctionTreeExtractor.getAggregateInputFunctionsWithoutKey(aggregateExpr.expr)
    val aggregateFunctionOperations = aggregateExpressions.zip(aggregateExpressionInputMaps)
      .map { case (expr, fun) => this.generateScanOperationForAggregateExpression(inputStream.keyType, expr, fun) }

    val operationOutputTypes = aggregateFunctionOperations.map(_.outputType)
    val operationOutputTupleType = TypeDescriptor.createTuple(operationOutputTypes)

    val combineOperationOutputsFunction = AggregateFunctionTreeExtractor.getResultTupleToOutputFunctionWithoutKey(aggregateExpr.expr)
      .withArgumentTypes(List(operationOutputTupleType))
    TypeChecker.typeCheck(combineOperationOutputsFunction)

    this.generateCombinedScanOperations(context.outputs, inputStream, aggregateFunctionOperations, combineOperationOutputsFunction)
  }

  private def generateScanOperationForAggregateWithKey(context: GeneratorContext,
                                                       inputStream: StreamInfo,
                                                       aggregateExpr: Aggregate): ScanOperationInlineClassInfo = {
    // The map function will be made up of zero or more function calls to aggregate functions, combined together
    // in some expression tree.
    // We need to separate out those aggregate function calls and create a flink AggregateFunction for each one.
    // Each of these separate AggregateFunctions will have the same input type, which is the input stream record type.
    val aggregateExpressions = AggregateFunctionTreeExtractor.getAggregateExpressions(aggregateExpr.expr)
    val aggregateExpressionInputMaps = AggregateFunctionTreeExtractor.getAggregateInputFunctionsWithKey(aggregateExpr.expr)
    val aggregateFunctionOperations = aggregateExpressions.zip(aggregateExpressionInputMaps)
      .map { case (expr, fun) => this.generateScanOperationForAggregateExpression(inputStream.keyType, expr, fun) }

    val operationOutputTypes = aggregateFunctionOperations.map(_.outputType)
    val operationOutputTupleType = TypeDescriptor.createTuple(operationOutputTypes)

    val combineOperationOutputsFunction = AggregateFunctionTreeExtractor.getResultTupleToOutputFunctionWithKey(aggregateExpr.expr)
      .withArgumentTypes(List(aggregateExpr.expr.arguments.head.tpe, operationOutputTupleType))
    TypeChecker.typeCheck(combineOperationOutputsFunction)

    this.generateCombinedScanOperations(context.outputs, inputStream, aggregateFunctionOperations, combineOperationOutputsFunction)
  }
}
