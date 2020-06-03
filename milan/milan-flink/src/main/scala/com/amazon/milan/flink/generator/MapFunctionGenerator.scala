package com.amazon.milan.flink.generator

import com.amazon.milan.flink.runtime.MapFunctionWithLineage
import com.amazon.milan.program.StreamMap


trait MapFunctionGenerator
  extends LineageRecordFactoryGenerator
    with MetricFactoryGenerator
    with KeyedStreamGenerator
    with FunctionGenerator {

  val typeLifter: TypeLifter

  import typeLifter._

  /**
   * Generates the code that applies a [[StreamMap]] to a data stream.
   *
   * @param context     The generator context.
   * @param mapExpr     The map expression to apply.
   * @param inputStream The stream to which the map expression will be applied.
   * @return The resulting mapped stream.
   */
  def applyMapExpression(context: GeneratorContext,
                         mapExpr: StreamMap,
                         inputStream: GeneratedDataStream): GeneratedDataStream = {
    inputStream match {
      case keyedStream: GeneratedKeyedDataStream if keyedStream.isContextual =>
        this.applyKeyedMapExpression(context, mapExpr, keyedStream)

      case dataStream: GeneratedDataStream =>
        this.applyUnkeyedMapExpression(context, mapExpr, dataStream)
    }
  }

  /**
   * Applies a map operation on a grouping, which produces another grouping of the mapped records.
   *
   * @param context     The generator context.
   * @param mapExpr     The map expression to apply.
   * @param inputStream The input grouped stream.
   * @return The resulting mapped grouped stream.
   */
  def applyMapGroup(context: GeneratorContext,
                    mapExpr: StreamMap,
                    inputStream: GeneratedKeyedDataStream): GeneratedKeyedDataStream = {
    val groupStreamTerm = mapExpr.expr.arguments(1).name
    val mapContext = context.withStreamTerm(groupStreamTerm, inputStream.toContextual)

    // Perform the operations defined in the body of the map function.
    val mappedStream = mapContext.getOrGenerateStream(mapExpr.expr.body)

    // If those operations left us without a keyed stream we need to key it again.
    mappedStream match {
      case keyedStream: GeneratedKeyedDataStream =>
        keyedStream

      case unkeyedStream: GeneratedUnkeyedDataStream =>
        this.keyStreamByRecordKey(context.output, unkeyedStream, mapExpr.nodeName)

      case _ =>
        throw new FlinkGeneratorException(s"Map operation did not result in a data stream: ${mapExpr.expr.body}")
    }
  }

  /**
   * Generates a class that implements a Flink MapFunction for the specified mapping expression and returns the class
   * name.
   *
   * @param context     The generator context.
   * @param mapExpr     A map expression.
   * @param inputStream The input stream being mapped.
   * @return The name of the generated class.
   */
  def generateMapFunction(context: GeneratorContext,
                          mapExpr: StreamMap,
                          inputStream: GeneratedStream): ClassName = {
    val className = context.output.newClassName(mapExpr, "MapFunction")

    val inputRecordType = inputStream.recordType
    val keyType = inputStream.keyType
    val outputRecordType = mapExpr.recordType.toFlinkRecordType

    val mapFunctionCode = this.getFunctionDefinition(
      context.output,
      "mapValue",
      mapExpr.expr.withArgumentTypes(List(inputRecordType)),
      outputRecordType)

    val classDef =
      q"""
         |class $className
         |  extends ${nameOf[MapFunctionWithLineage[Any, Product, Any]]}[${inputRecordType.toFlinkTerm}, ${keyType.toTerm}, ${outputRecordType.toFlinkTerm}](
         |    ${liftTypeDescriptorToTypeInformation(outputRecordType)},
         |    ${liftTypeDescriptorToTypeInformation(keyType)},
         |    ${this.generateLineageRecordFactory(context, mapExpr, inputStream)},
         |    ${context.output.lineageOutputTag},
         |    ${this.generateMetricFactory(context, mapExpr)}) {
         |
         |  protected override ${mapFunctionCode.indentTail(1)}
         |}
         |""".strip

    context.output.addClassDef(classDef)

    className
  }

  private def applyUnkeyedMapExpression(context: GeneratorContext,
                                        mapExpr: StreamMap,
                                        inputStream: GeneratedDataStream): GeneratedUnkeyedDataStream = {
    val streamVal = context.output.newStreamValName(mapExpr)

    val mapFunctionVal = context.output.newValName(mapExpr, "mapFunction")
    val mapFunctionClassName = this.generateMapFunction(context, mapExpr, inputStream)

    val codeBlock =
      q"""
         |val $mapFunctionVal = new $mapFunctionClassName
         |val $streamVal = ${inputStream.streamVal}.map($mapFunctionVal)
         |""".strip

    context.output.appendMain(codeBlock)

    // Changing the record type un-keys the stream.
    GeneratedUnkeyedDataStream(mapExpr.nodeId, streamVal, mapExpr.recordType.toFlinkRecordType, inputStream.keyType, isContextual = false)
  }

  private def applyKeyedMapExpression(context: GeneratorContext,
                                      mapExpr: StreamMap,
                                      inputStream: GeneratedKeyedDataStream): GeneratedKeyedDataStream = {
    throw new NotImplementedError()
  }
}
