package com.amazon.milan.flink.generator

import com.amazon.milan.flink.runtime.{KeyedLastByOperator, UnkeyedLastByOperator}
import com.amazon.milan.program.{FunctionDef, GreaterThan, Last, SelectField, SelectTerm, TypeChecker, ValueDef}


trait LastByGenerator
  extends KeyedStreamGenerator {

  val typeLifter: TypeLifter

  import typeLifter._

  def applyLastOperation(context: GeneratorContext,
                         lastExpr: Last): GeneratedDataStream = {
    context.getOrGenerateDataStream(lastExpr.source) match {
      case keyedStream: GeneratedKeyedDataStream =>
        this.applyLastOperation(context, keyedStream, lastExpr)

      case unkeyedStream: GeneratedUnkeyedDataStream =>
        this.applyLastOperation(context, unkeyedStream, lastExpr)
    }
  }

  def applyLastOperation(context: GeneratorContext,
                         inputStream: GeneratedUnkeyedDataStream,
                         lastExpr: Last): GeneratedUnkeyedDataStream = {
    this.applyLastOperationInternal(context, inputStream, lastExpr)
  }

  def applyLastOperation(context: GeneratorContext,
                         inputStream: GeneratedKeyedDataStream,
                         lastExpr: Last): GeneratedKeyedDataStream = {
    val unkeyedStream = this.applyLastOperationInternal(context, inputStream, lastExpr)
    this.keyStreamByRecordKey(context.output, unkeyedStream, unkeyedStream.streamId + "_keyed")
  }

  private def applyLastOperationInternal(context: GeneratorContext,
                                         inputStream: GeneratedDataStream,
                                         lastExpr: Last): GeneratedUnkeyedDataStream = {
    val recordType = inputStream.recordType
    val keyType = inputStream.keyType

    val takeNewValueFunction =
      FunctionDef(
        List(ValueDef("newRecord", recordType.wrappedWithKey(keyType)), ValueDef("currentRecord", recordType.wrappedWithKey(keyType))),
        GreaterThan(SelectField(SelectTerm("newRecord"), "sequenceNumber"), SelectField(SelectTerm("currentRecord"), "sequenceNumber"))
      )
    TypeChecker.typeCheck(takeNewValueFunction)

    val operatorClassName = this.generateLastByOperator(context.output, inputStream, takeNewValueFunction, lastExpr.nodeName)

    val operatorVal = context.output.newValName(s"stream_${lastExpr.nodeName}_lastOperator_")
    val outputStreamVal = context.output.newStreamValName(lastExpr)

    val operatorName = s"${lastExpr.nodeName}_lastOperator"

    // If the stream is not keyed then we need to remove any parallelism, otherwise we'll get multiple outputs.
    val setParallelism =
      inputStream match {
        case _: GeneratedKeyedDataStream => qc""
        case _ => qc".setParallelism(1)"
      }

    val codeBlock =
      q"""
         |val $operatorVal = new $operatorClassName
         |val $outputStreamVal = ${inputStream.streamVal}.transform($operatorName, $operatorVal.getProducedType, $operatorVal)$setParallelism
         |""".strip

    context.output.appendMain(codeBlock)

    GeneratedUnkeyedDataStream(inputStream.streamId, outputStreamVal, recordType, keyType, inputStream.isContextual)
  }

  private def generateLastByOperator(env: GeneratorOutputs,
                                     inputStream: GeneratedDataStream,
                                     takeNewValueFunction: FunctionDef,
                                     streamIdentifier: String): ClassName = {

    val className = env.newClassName(s"LastByOperator_${streamIdentifier}_")

    val recordType = inputStream.recordType
    val keyType = inputStream.keyType

    val takeNewValueFunctionDef = env.scalaGenerator.getScalaFunctionDef("takeNewValue", takeNewValueFunction)

    val baseClassName =
      inputStream match {
        case _: GeneratedKeyedDataStream =>
          nameOf[KeyedLastByOperator[Any, Product]]

        case _: GeneratedUnkeyedDataStream =>
          nameOf[UnkeyedLastByOperator[Any, Product]]
      }

    val classDef =
      q"""
         |class $className extends $baseClassName[${recordType.toFlinkTerm}, ${keyType.toTerm}](
         |  ${liftTypeDescriptorToTypeInformation(recordType)},
         |  ${liftTypeDescriptorToTypeInformation(keyType)}) {
         |
         |  override ${code(takeNewValueFunctionDef).indentTail(1)}
         |
         |}
         |""".strip

    env.addClassDef(classDef)

    className
  }


}
