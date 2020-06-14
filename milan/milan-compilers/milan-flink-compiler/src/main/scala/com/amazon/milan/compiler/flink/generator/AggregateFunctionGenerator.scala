package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.compiler.scala._
import com.amazon.milan.compiler.flink.internal.AggregateFunctionTreeExtractor
import com.amazon.milan.compiler.flink.runtime.{BuiltinAggregateFunctions, _}
import com.amazon.milan.compiler.flink.types._
import com.amazon.milan.program.{Aggregate, AggregateExpression, ArgAggregateExpression, ArgMax, ArgMin, Count, First, FunctionDef, GroupingExpression, Max, Mean, Min, Sum, TimeWindowExpression, TypeChecker}
import com.amazon.milan.typeutil.{TupleTypeDescriptor, TypeDescriptor, types}


trait AggregateFunctionGenerator extends FunctionGenerator {
  val typeLifter: FlinkTypeLifter

  import typeLifter._

  /**
   * Applies a select() operation that is being performed on a Flink WindowedStream stream.
   *
   * @param outputs        The generator output collector.
   * @param aggExpr        The [[Aggregate]] expression representing the select() operation.
   * @param windowedStream The windowed stream on which the operation is being performed.
   * @return A [[GeneratedUnkeyedDataStream]] describing the output stream of the operation.
   */
  def applySelectToWindowedStream(outputs: GeneratorOutputs,
                                  aggExpr: Aggregate,
                                  windowedStream: GeneratedKeyedWindowedStream): GeneratedUnkeyedDataStream = {
    val outputRecordType = aggExpr.recordType.toFlinkRecordType

    val processWindowFunctionClassName = this.generateProcessWindowFunction(outputs, windowedStream, aggExpr, windowedStream.windowKeyType)
    val aggregateFunctionInfo = this.generateIntermediateAggregateFunction(outputs, aggExpr, windowedStream.recordType, windowedStream.keyType)
    val processWindowFunctionVal = outputs.newValName(s"stream_${aggExpr.nodeName}_processWindowFunction_")
    val aggregateFunctionVal = outputs.newValName(s"stream_${aggExpr.nodeName}_aggregateFunction_")
    val outputStreamVal = outputs.newStreamValName(aggExpr)

    val accumulatorTypeInfoVal = outputs.newValName(s"stream_${aggExpr.nodeName}_accumulatorTypeInfo_")
    val outputTypeInfoVal = outputs.newValName(s"stream_${aggExpr.nodeName}_outputTypeInfo_")

    outputs.appendMain(
      q"""
         |val $processWindowFunctionVal = new $processWindowFunctionClassName()
         |val $aggregateFunctionVal = new ${aggregateFunctionInfo.className}()
         |val $accumulatorTypeInfoVal = ${liftTypeDescriptorToTypeInformation(aggregateFunctionInfo.accumulatorType)}
         |val $outputTypeInfoVal = ${liftTypeDescriptorToTypeInformation(outputRecordType.wrapped)}
         |val $outputStreamVal = ${windowedStream.streamVal}.aggregate(
         |  $aggregateFunctionVal,
         |  $processWindowFunctionVal,
         |  $accumulatorTypeInfoVal,
         |  $aggregateFunctionVal.getProducedType,
         |  $outputTypeInfoVal)
         |""".strip)

    GeneratedUnkeyedDataStream(aggExpr.nodeId, outputStreamVal, aggExpr.recordType.toFlinkRecordType, types.EmptyTuple, isContextual = false)
  }

  /**
   * Applies a select() operation that is being performed on a Flink AllWindowedStream stream.
   *
   * @param outputs        The generator output collector.
   * @param aggExpr        The [[Aggregate]] expression representing the select() operation.
   * @param windowedStream The windowed stream on which the operation is being performed.
   * @return A [[GeneratedUnkeyedDataStream]] describing the output stream of the operation.
   */
  def applySelectToAllWindowedStream(outputs: GeneratorOutputs,
                                     aggExpr: Aggregate,
                                     windowedStream: GeneratedUnkeyedWindowStream): GeneratedUnkeyedDataStream = {
    val processWindowFunctionClassName = this.generateProcessAllWindowFunction(outputs, aggExpr)
    val aggregateFunctionInfo = this.generateIntermediateAggregateFunction(outputs, aggExpr, windowedStream.recordType, windowedStream.keyType)
    val outputRecordType = aggExpr.recordType.toFlinkRecordType

    val processWindowFunctionVal = outputs.newValName(s"stream_${aggExpr.nodeName}_processWindowFunction_")
    val aggregateFunctionVal = outputs.newValName(s"stream_${aggExpr.nodeName}_aggregateFunction_")
    val outputStreamVal = outputs.newStreamValName(aggExpr)

    val accumulatorTypeInfoVal = outputs.newValName(s"stream_${aggExpr.nodeName}_accumulatorTypeInfo_")
    val aggregateOutputTypeInfoVal = outputs.newValName(s"stream_${aggExpr.nodeName}_intermediateOutputTypeInfo_")
    val outputTypeInfoVal = outputs.newValName(s"stream_${aggExpr.nodeName}_outputTypeInfo_")

    val codeBlock =
      q"""
         |val $processWindowFunctionVal = new $processWindowFunctionClassName()
         |val $aggregateFunctionVal = new ${aggregateFunctionInfo.className}()
         |val $accumulatorTypeInfoVal = ${liftTypeDescriptorToTypeInformation(aggregateFunctionInfo.accumulatorType)}
         |val $aggregateOutputTypeInfoVal = ${liftTypeDescriptorToTypeInformation(aggregateFunctionInfo.outputType)}
         |val $outputTypeInfoVal = ${liftTypeDescriptorToTypeInformation(outputRecordType.wrapped)}
         |val $outputStreamVal = ${windowedStream.streamVal}.aggregate(
         |  $aggregateFunctionVal,
         |  $processWindowFunctionVal,
         |  $accumulatorTypeInfoVal,
         |  $aggregateOutputTypeInfoVal,
         |  $outputTypeInfoVal)
         |""".strip

    outputs.appendMain(codeBlock)

    GeneratedUnkeyedDataStream(aggExpr.nodeId, outputStreamVal, outputRecordType, types.EmptyTuple, isContextual = false)
  }

  private def generateIntermediateAggregateFunction(outputs: GeneratorOutputs,
                                                    aggExpr: Aggregate,
                                                    inputRecordType: TypeDescriptor[_],
                                                    inputKeyType: TypeDescriptor[_]): AggregateFunctionClassInfo = {
    // The map function will be made up of zero or more function calls to aggregate functions, combined together
    // in some expression tree.
    // We need to separate out those aggregate function calls and create a flink AggregateFunction for each one.
    // Each of these separate AggregateFunctions will have the same input type, which is the input stream record type.
    val aggregateExpressions = AggregateFunctionTreeExtractor.getAggregateExpressions(aggExpr.expr)
    val aggregateExpressionInputMaps = AggregateFunctionTreeExtractor.getAggregateInputFunctions(aggExpr.expr)
    val aggregateFunctionClassInfos = aggregateExpressions.zip(aggregateExpressionInputMaps)
      .map { case (expr, fun) => this.generateAggregateFunctionForExpression(outputs, aggExpr.nodeName, inputRecordType, expr, fun) }

    val combinedOutputType = TypeDescriptor.createTuple(aggregateFunctionClassInfos.map(_.outputType))

    // If the aggregate function we're generating is for a named field then don't wrap the output in an
    // AggregatorOutputRecord, because it will be wrapped later on.
    val aggregateOutputType = AggregatorOutputRecord.createTypeDescriptor(combinedOutputType)

    val combinedAccumulatorType = TypeDescriptor.createTuple(aggregateFunctionClassInfos.map(_.accumulatorType))
    val className = outputs.newClassName(s"AggregateFunction_${aggExpr.nodeName}_")

    val innerTypes = aggregateFunctionClassInfos.map(c => (c.accumulatorType, c.outputType))
    val baseClassFullName = this.getMultiAggregateFunctionFullName(inputRecordType, inputKeyType, innerTypes)
    val aggregateFunctionCreationStatements = aggregateFunctionClassInfos.map(c => s"new ${c.className}()").mkString(",\n")

    val classDef =
      q"""
         |class $className
         |  extends $baseClassFullName(
         |    ${code(aggregateFunctionCreationStatements.indentTail(1))})
         |""".strip

    outputs.addClassDef(classDef)

    AggregateFunctionClassInfo(className, combinedAccumulatorType, aggregateOutputType)
  }

  private def getMultiAggregateFunctionFullName(inputRecordType: TypeDescriptor[_],
                                                inputKeyType: TypeDescriptor[_],
                                                innerTypes: List[(TypeDescriptor[_], TypeDescriptor[_])]): CodeBlock = {
    val baseClassName =
      ClassName(nameOf[MultiAggregateFunction0[Any, Product]].value.substring(0, nameOf[MultiAggregateFunction0[Any, Product]].value.length - 1) + innerTypes.length.toString)

    if (innerTypes.nonEmpty) {
      val typeArgs = innerTypes.map { case (accumulatorType, outputType) => s"${accumulatorType.toFlinkTerm}, ${outputType.toFlinkTerm}" }.mkString(", ")
      qc"$baseClassName[${inputRecordType.toFlinkTerm}, ${inputKeyType.toTerm}, ${code(typeArgs)}]"
    }
    else {
      qc"$baseClassName[${inputRecordType.toFlinkTerm}, ${inputKeyType.toTerm}]"
    }
  }

  /**
   * Generates the Flink aggregate function for a single aggregate expression.
   */
  private def generateAggregateFunctionForExpression(outputs: GeneratorOutputs,
                                                     streamIdentifier: String,
                                                     inputType: TypeDescriptor[_],
                                                     aggregateExpression: AggregateExpression,
                                                     inputMappingFunctionDef: FunctionDef): AggregateFunctionClassInfo = {
    val mappedInputTypes =
      inputMappingFunctionDef.tpe match {
        case TupleTypeDescriptor(types) => types
        case t => List(t)
      }

    val mappedType = inputMappingFunctionDef.tpe
    val outputType = aggregateExpression.tpe

    outputs.addImport(com.amazon.milan.compiler.flink.runtime.implicits.packageName + "._")

    val innerAggregateFunctionInfo = this.getFlinkAggregateFunctionCreationStatement(mappedInputTypes, aggregateExpression)
    val accumulatorType = innerAggregateFunctionInfo.accumulatorType

    val mapInputDef = outputs.scalaGenerator.getScalaFunctionDef("mapInput", inputMappingFunctionDef)

    val className = outputs.newClassName(s"AggregateFunction_${streamIdentifier}_${aggregateExpression.expressionType}_")

    val classDef =
      q"""
         |class $className
         |  extends ${nameOf[InputMappingAggregateFunctionWrapper[Any, Any, Any, Any]]}[${inputType.toFlinkTerm}, ${mappedType.toFlinkTerm}, ${accumulatorType.toFlinkTerm}, ${outputType.toFlinkTerm}](
         |    ${innerAggregateFunctionInfo.creationStatement}) {
         |
         |  override ${code(mapInputDef.indentTail(1))}
         |}
         |""".strip

    outputs.addClassDef(classDef)

    AggregateFunctionClassInfo(className, accumulatorType, outputType)
  }

  private def getFlinkAggregateFunctionCreationStatement(inputTypes: List[TypeDescriptor[_]],
                                                         aggregateExpression: AggregateExpression): AggregateFunctionCreationInfo = {
    aggregateExpression match {
      case _: ArgAggregateExpression =>
        this.getFlinkArgAggregateFunctionCreationStatement(inputTypes.head, inputTypes.last, aggregateExpression)

      case _: First =>
        val creationStatement = qc"new ${nameOf[BuiltinAggregateFunctions.Any[Any]]}[${inputTypes.head.toFlinkTerm}](${liftTypeDescriptorToTypeInformation(inputTypes.head)})"
        val accumulatorType = TypeDescriptor.optionOf(inputTypes.head)
        AggregateFunctionCreationInfo(creationStatement, accumulatorType)

      case _: Count =>
        AggregateFunctionCreationInfo(qc"new ${nameOf[BuiltinAggregateFunctions.Count]}", types.Long)

      case _ =>
        this.getFlinkNumericAggregateFunctionCreationStatement(inputTypes.head, aggregateExpression)
    }
  }

  private def getFlinkArgAggregateFunctionCreationStatement(argType: TypeDescriptor[_],
                                                            inputType: TypeDescriptor[_],
                                                            aggregateExpression: AggregateExpression): AggregateFunctionCreationInfo = {
    val argTypeInfo = liftTypeDescriptorToTypeInformation(argType)
    val inputTypeInfo = liftTypeDescriptorToTypeInformation(inputType)

    val creationStatement =
      aggregateExpression match {
        case _: ArgMax => qc"new ${nameOf[BuiltinAggregateFunctions.ArgMax[Any, Any]]}[${argType.toFlinkTerm}, ${inputType.toFlinkTerm}]($argTypeInfo, $inputTypeInfo)"
        case _: ArgMin => qc"new ${nameOf[BuiltinAggregateFunctions.ArgMin[Any, Any]]}[${argType.toFlinkTerm}, ${inputType.toFlinkTerm}]($argTypeInfo, $inputTypeInfo)"
        case _ => throw new FlinkGeneratorException(s"Unsupported aggregation operation {$aggregateExpression.name}.")
      }

    val accumulatorType = TypeDescriptor.createTuple(List(TypeDescriptor.optionOf(argType), TypeDescriptor.optionOf(inputType)))
    AggregateFunctionCreationInfo(creationStatement, accumulatorType)
  }

  private def getFlinkNumericAggregateFunctionCreationStatement(inputType: TypeDescriptor[_],
                                                                aggregateExpression: AggregateExpression): AggregateFunctionCreationInfo = {
    val inputTypeInfo = liftTypeDescriptorToTypeInformation(inputType)

    val creationStatement =
      aggregateExpression match {
        case _: Sum => qc"new ${nameOf[BuiltinAggregateFunctions.Sum[Any]]}[${inputType.toFlinkTerm}]($inputTypeInfo)"
        case _: Min => qc"new ${nameOf[BuiltinAggregateFunctions.Min[Any]]}[${inputType.toFlinkTerm}]($inputTypeInfo)"
        case _: Max => qc"new ${nameOf[BuiltinAggregateFunctions.Max[Any]]}[${inputType.toFlinkTerm}]($inputTypeInfo)"
        case _: Mean => qc"new ${nameOf[BuiltinAggregateFunctions.Mean[Any]]}[${inputType.toFlinkTerm}]($inputTypeInfo)"
        case _ => throw new FlinkGeneratorException(s"Unsupported aggregation operation {$aggregateExpression.name}.")
      }

    val accumulatorType = TypeDescriptor.optionOf(inputType)
    AggregateFunctionCreationInfo(creationStatement, accumulatorType)
  }

  private def generateProcessWindowFunction(output: GeneratorOutputs,
                                            windowedStream: GeneratedWindowedStream,
                                            aggExpr: Aggregate,
                                            groupKeyType: TypeDescriptor[_]): ClassName = {
    val mapFunctionDef = aggExpr.expr
    val accumulatorType = this.getAccumulatorType(mapFunctionDef)
    val outputRecordType = aggExpr.recordType.toFlinkRecordType

    val baseClassName =
      aggExpr.source match {
        case TimeWindowExpression(GroupingExpression(_, _), _, _, _) =>
          qn"${nameOf[TimeWindowGroupByProcessWindowFunction[Any, Any, Any]]}[${accumulatorType.toFlinkTerm}, ${windowedStream.keyType.toTerm}, ${outputRecordType.toFlinkTerm}]"

        case _ =>
          qn"${nameOf[GroupByProcessWindowFunction[Any, Any, Any, Any]]}[${accumulatorType.toFlinkTerm}, ${windowedStream.keyType.toTerm}, ${groupKeyType.toTerm}, ${outputRecordType.toFlinkTerm}]"
      }

    val className = output.newClassName(aggExpr, "ProcessWindowFunction")

    val getKeyFunctionDef = KeyExtractorUtils.getRecordKeyToKeyFunction(windowedStream, aggExpr)
    val getKeyDef = output.scalaGenerator.getScalaFunctionDef("getKey", getKeyFunctionDef)

    // Get the Milan FunctionDef for the function that converts the output from the combined aggregation
    // function to the final output record type.
    val outputMapFunctionDef = AggregateFunctionTreeExtractor.getResultTupleToOutputFunction(mapFunctionDef)
      .withArgumentTypes(List(groupKeyType, accumulatorType))
    TypeChecker.typeCheck(outputMapFunctionDef)
    val getOutputDef = getFunctionDefinition(output, "getOutput", outputMapFunctionDef, outputRecordType)

    val classDef =
      q"""
         |class $className extends $baseClassName {
         |  override ${code(getKeyDef).indentTail(1)}
         |
         |  override ${getOutputDef.indentTail(1)}
         |}
         |""".strip

    output.addClassDef(classDef)

    className
  }

  private def generateProcessAllWindowFunction(output: GeneratorOutputs,
                                               aggExpr: Aggregate): ClassName = {
    val className = output.newClassName(aggExpr, "ProcessAllWindowFunction")

    val mapFunctionDef = aggExpr.expr
    val keyType = types.Instant
    val outputRecordType = aggExpr.recordType.toFlinkRecordType
    val accumulatorType = this.getAccumulatorType(mapFunctionDef)

    // Get the Milan FunctionDef for the function that converts the output from the combined aggregation
    // function to the final output record type.
    val outputMapFunctionDef = AggregateFunctionTreeExtractor.getResultTupleToOutputFunction(mapFunctionDef)
      .withArgumentTypes(List(keyType, accumulatorType))

    TypeChecker.typeCheck(outputMapFunctionDef)
    val getOutputDef = this.getFunctionDefinition(output, "getOutput", outputMapFunctionDef, outputRecordType)

    val classDef =
      q"""
         |class $className extends ${nameOf[TimeWindowProcessAllWindowFunction[Any, Any]]}[${accumulatorType.toFlinkTerm}, ${outputRecordType.toFlinkTerm}] {
         |  override ${getOutputDef.indentTail(1)}
         |}
         |""".strip

    output.addClassDef(classDef)

    className
  }

  private def getAccumulatorType(mapFunctionDef: FunctionDef): TypeDescriptor[_] = {
    val aggregateFunctionReferences = AggregateFunctionTreeExtractor.getAggregateExpressions(mapFunctionDef)
    val aggregateFunctionReturnTypes = aggregateFunctionReferences.map(_.tpe)
    TypeDescriptor.createTuple[Any](aggregateFunctionReturnTypes)
  }

  case class AggregateFunctionCreationInfo(creationStatement: CodeBlock, accumulatorType: TypeDescriptor[_])

  case class AggregateFunctionClassInfo(className: ClassName, accumulatorType: TypeDescriptor[_], outputType: TypeDescriptor[_])

}
