package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.compiler.scala._
import com.amazon.milan.compiler.flink.runtime.{FullJoinKeyedCoProcessFunction, LeftInnerJoinKeyedCoProcessFunction, LeftJoinKeyedCoProcessFunction}
import com.amazon.milan.program.{FullJoin, FunctionDef, JoinExpression, LeftInnerJoin, LeftJoin, StreamMap}
import com.amazon.milan.typeutil.TypeDescriptor

import scala.language.existentials


trait CoProcessFunctionGenerator
  extends RecordIdExtractorGenerator
    with LineageRecordFactoryGenerator
    with MetricFactoryGenerator
    with FunctionGenerator {

  val typeLifter: FlinkTypeLifter

  import typeLifter._

  def applyKeyedCoProcessFunction(context: GeneratorContext,
                                  mapExpr: StreamMap,
                                  leftInputStream: GeneratedStream,
                                  rightInputStream: GeneratedStream,
                                  keyType: TypeDescriptor[_],
                                  connectedStreamsVal: ValName,
                                  joinPostCondition: Option[FunctionDef]): GeneratedUnkeyedDataStream = {
    val coProcessFunctionClassName = this.generateKeyedCoProcessFunction(context, mapExpr, leftInputStream, rightInputStream, keyType, joinPostCondition)
    val outputRecordType = mapExpr.recordType.toFlinkRecordType

    val coProcessFunctionVal = context.output.newValName(s"stream_${mapExpr.nodeName}_coProcessFunction_")
    val outputTypeInfoVal = context.output.newValName(s"stream_${mapExpr.nodeName}_recordTypeInfo_")
    val outputStreamVal = context.output.newStreamValName(mapExpr)

    context.output.appendMain(
      q"""
         |val $coProcessFunctionVal = new $coProcessFunctionClassName()
         |val $outputTypeInfoVal = $coProcessFunctionVal.getProducedType
         |val $outputStreamVal = $connectedStreamsVal.process($coProcessFunctionVal, $outputTypeInfoVal)
         |""".codeStrip)

    GeneratedUnkeyedDataStream(mapExpr.nodeName, outputStreamVal, outputRecordType, keyType, isContextual = false)
  }

  private def generateKeyedCoProcessFunction(context: GeneratorContext,
                                             mapExpr: StreamMap,
                                             leftInputStream: GeneratedStream,
                                             rightInputStream: GeneratedStream,
                                             keyType: TypeDescriptor[_],
                                             joinPostCondition: Option[FunctionDef]): ClassName = {
    val joinExpr =
      mapExpr.source match {
        case joinExpr: JoinExpression =>
          joinExpr

        case unsupported =>
          throw new FlinkGeneratorException(s"Unsupported input stream type to CoProcessFunction: ${unsupported.expressionType}.")
      }

    joinExpr match {
      case _: LeftJoin =>
        this.generateLeftJoinKeyedCoProcessFunction(context, mapExpr, leftInputStream, rightInputStream, keyType, joinPostCondition)

      case _: FullJoin =>
        this.generateFullJoinKeyedCoProcessFunction(context, mapExpr, leftInputStream, rightInputStream, keyType, joinPostCondition)

      case _: LeftInnerJoin =>
        this.generateLeftInnerJoinKeyedCoProcessFunction(context, mapExpr, leftInputStream, rightInputStream, keyType, joinPostCondition)

      case unsupported =>
        throw new FlinkGeneratorException(s"Unsupported join type for CoProcessFunction: ${unsupported.expressionType}.")
    }
  }

  private def generateLeftJoinKeyedCoProcessFunction(context: GeneratorContext,
                                                     mapExpr: StreamMap,
                                                     leftInputStream: GeneratedStream,
                                                     rightInputStream: GeneratedStream,
                                                     keyType: TypeDescriptor[_],
                                                     joinPostCondition: Option[FunctionDef]): ClassName = {
    val className = context.output.newClassName(s"CoProcessFunction_LeftJoin_${mapExpr.nodeName}")
    val leftRecordType = leftInputStream.recordType
    val rightRecordType = rightInputStream.recordType
    val outputRecordType = mapExpr.recordType.toFlinkRecordType
    val rightTypeInfo = liftTypeDescriptorToTypeInformation(rightRecordType)
    val keyTypeInfo = liftTypeDescriptorToTypeInformation(keyType)
    val outputTypeInfo = liftTypeDescriptorToTypeInformation(outputRecordType)

    val mapFunctionDef = this.getMapFunctionDef(context.output, mapExpr, leftRecordType, rightRecordType)
    val postConditionDef = this.getPostConditionDef(context.output, joinPostCondition, leftRecordType, rightRecordType)

    val classDef =
      q"""
         |class $className
         |  extends ${nameOf[LeftJoinKeyedCoProcessFunction[Any, Any, Product, Any]]}[${leftRecordType.toFlinkTerm}, ${rightRecordType.toFlinkTerm}, ${keyType.toTerm}, ${outputRecordType.toFlinkTerm}](
         |    $rightTypeInfo,
         |    $keyTypeInfo,
         |    $outputTypeInfo,
         |    new ${this.generateRecordIdExtractor(context.output, leftInputStream.recordType)}(),
         |    new ${this.generateRecordIdExtractor(context.output, rightInputStream.recordType)}(),
         |    new ${this.generateRecordIdExtractor(context.output, outputRecordType)}(),
         |    ${this.generateJoinLineageRecordFactory(context, mapExpr, leftInputStream, rightInputStream)},
         |    ${context.output.lineageOutputTag},
         |    ${this.generateMetricFactory(context, mapExpr)}) {
         |
         |    override ${mapFunctionDef.indentTail(1)}
         |
         |    override ${postConditionDef.indentTail(1)}
         |}
         |""".codeStrip

    context.output.addClassDef(classDef)

    className
  }

  private def generateFullJoinKeyedCoProcessFunction(context: GeneratorContext,
                                                     mapExpr: StreamMap,
                                                     leftInputStream: GeneratedStream,
                                                     rightInputStream: GeneratedStream,
                                                     keyType: TypeDescriptor[_],
                                                     joinPostCondition: Option[FunctionDef]): ClassName = {
    val className = context.output.newClassName(s"CoProcessFunction_FullJoin_${mapExpr.nodeName}")
    val leftRecordType = leftInputStream.recordType
    val rightRecordType = rightInputStream.recordType
    val outputRecordType = mapExpr.recordType.toFlinkRecordType
    val leftTypeInfo = liftTypeDescriptorToTypeInformation(leftRecordType)
    val rightTypeInfo = liftTypeDescriptorToTypeInformation(rightRecordType)
    val keyTypeInfo = liftTypeDescriptorToTypeInformation(keyType)
    val outputTypeInfo = liftTypeDescriptorToTypeInformation(outputRecordType)

    val mapFunctionDef = this.getMapFunctionDef(context.output, mapExpr, leftRecordType, rightRecordType)
    val postConditionDef = this.getPostConditionDef(context.output, joinPostCondition, leftRecordType, rightRecordType)

    val classDef =
      q"""
         |class $className
         |  extends ${nameOf[FullJoinKeyedCoProcessFunction[Any, Any, Product, Any]]}[${leftRecordType.toFlinkTerm}, ${rightRecordType.toFlinkTerm}, ${keyType.toTerm}, ${outputRecordType.toFlinkTerm}](
         |    $leftTypeInfo,
         |    $rightTypeInfo,
         |    $keyTypeInfo,
         |    $outputTypeInfo,
         |    new ${this.generateRecordIdExtractor(context.output, leftRecordType)}(),
         |    new ${this.generateRecordIdExtractor(context.output, rightRecordType)}(),
         |    new ${this.generateRecordIdExtractor(context.output, outputRecordType)}(),
         |    ${this.generateJoinLineageRecordFactory(context, mapExpr, leftInputStream, rightInputStream)},
         |    ${context.output.lineageOutputTag},
         |    ${this.generateMetricFactory(context, mapExpr)}) {
         |
         |  override ${mapFunctionDef.indentTail(1)}
         |
         |  override ${postConditionDef.indentTail(1)}
         |}
         |""".codeStrip

    context.output.addClassDef(classDef)

    className
  }

  private def generateLeftInnerJoinKeyedCoProcessFunction(context: GeneratorContext,
                                                          mapExpr: StreamMap,
                                                          leftInputStream: GeneratedStream,
                                                          rightInputStream: GeneratedStream,
                                                          keyType: TypeDescriptor[_],
                                                          joinPostCondition: Option[FunctionDef]): ClassName = {
    val className = context.output.newClassName(s"CoProcessFunction_LeftInnerJoin_${mapExpr.nodeName}")
    val leftRecordType = leftInputStream.recordType
    val rightRecordType = rightInputStream.recordType
    val outputRecordType = mapExpr.recordType.toFlinkRecordType
    val leftTypeInfo = liftTypeDescriptorToTypeInformation(leftRecordType)
    val rightTypeInfo = liftTypeDescriptorToTypeInformation(rightRecordType)
    val keyTypeInfo = liftTypeDescriptorToTypeInformation(keyType)
    val outputTypeInfo = liftTypeDescriptorToTypeInformation(outputRecordType)

    val mapFunctionDef = this.getMapFunctionDef(context.output, mapExpr, leftRecordType, rightRecordType)
    val postConditionDef = this.getPostConditionDef(context.output, joinPostCondition, leftRecordType, rightRecordType)

    val classDef =
      q"""
         |class $className
         |  extends ${nameOf[LeftInnerJoinKeyedCoProcessFunction[Any, Any, Product, Any]]}[${leftRecordType.toFlinkTerm}, ${rightRecordType.toFlinkTerm}, ${keyType.toTerm}, ${outputRecordType.toFlinkTerm}](
         |    $leftTypeInfo,
         |    $rightTypeInfo,
         |    $keyTypeInfo,
         |    $outputTypeInfo,
         |    new ${this.generateRecordIdExtractor(context.output, leftRecordType)}(),
         |    new ${this.generateRecordIdExtractor(context.output, rightRecordType)}(),
         |    new ${this.generateRecordIdExtractor(context.output, outputRecordType)}(),
         |    ${this.generateJoinLineageRecordFactory(context, mapExpr, leftInputStream, rightInputStream)},
         |    ${context.output.lineageOutputTag},
         |    ${this.generateMetricFactory(context, mapExpr)}) {
         |
         |    override ${mapFunctionDef.indentTail(1)}
         |
         |    override ${postConditionDef.indentTail(1)}
         |}
         |""".codeStrip

    context.output.addClassDef(classDef)

    className
  }

  private def getMapFunctionDef(env: GeneratorOutputs,
                                mapExpr: StreamMap,
                                leftRecordType: TypeDescriptor[_],
                                rightRecordType: TypeDescriptor[_]): CodeBlock = {

    val outputRecordType = mapExpr.recordType.toFlinkRecordType

    this.getFunctionDefinition(
      env,
      "map",
      mapExpr.expr.withArgumentTypes(List(leftRecordType, rightRecordType)),
      outputRecordType)
  }

  private def getPostConditionDef(env: GeneratorOutputs,
                                  postCondition: Option[FunctionDef],
                                  leftRecordType: TypeDescriptor[_],
                                  rightRecordType: TypeDescriptor[_]): CodeBlock = {
    postCondition match {
      case Some(f) =>
        code(env.scalaGenerator.getScalaFunctionDef("postCondition", f))

      case None =>
        qc"def postCondition(left: ${leftRecordType.toFlinkTerm}, right: ${rightRecordType.toFlinkTerm}): Boolean = true"
    }
  }
}
