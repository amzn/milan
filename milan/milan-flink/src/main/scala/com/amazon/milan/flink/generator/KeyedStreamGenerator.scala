package com.amazon.milan.flink.generator

import com.amazon.milan.flink.runtime.{ModifyRecordKeyMapFunction, RecordWrapperKeySelector}
import com.amazon.milan.program.{FunctionDef, GroupBy, TypeChecker}
import com.amazon.milan.typeutil.TypeDescriptor


trait KeyedStreamGenerator {
  val typeLifter: TypeLifter

  import typeLifter._

  /**
   * Applies a GroupBy operation.
   *
   * @param context     The generator context.
   * @param groupByExpr The GroupBy operation to apply.
   * @return A [[GeneratedKeyedDataStream]] containing the result of applying the operation.
   */
  def applyGroupBy(context: GeneratorContext,
                   groupByExpr: GroupBy): GeneratedKeyedDataStream = {
    val inputStream = context.getOrGenerateDataStream(groupByExpr.source)
    this.keyStreamByFunction(context.output, inputStream, groupByExpr.expr, groupByExpr.nodeName)
  }

  def keyStreamByFunction(outputs: GeneratorOutputs,
                          inputStream: GeneratedDataStream,
                          keyFunction: FunctionDef,
                          streamIdentifier: String): GeneratedKeyedDataStream = {
    TypeChecker.typeCheck(keyFunction)

    val recordType = inputStream.recordType

    val keyAppenderMapFunction = this.generateKeyAppenderMapFunction(outputs, streamIdentifier, recordType, inputStream.keyType, keyFunction)
    val keyType = keyAppenderMapFunction.outputKeyType

    val keyAssignerMapFunctionVal = outputs.newValName(s"stream_${streamIdentifier}_keyAssignerMapFunction_")
    val mappedStreamVal = outputs.newValName(s"stream_${streamIdentifier}_mappedWithKeys_")

    val codeBlock =
      q"""val $keyAssignerMapFunctionVal = new ${keyAppenderMapFunction.className}()
         |val $mappedStreamVal = ${inputStream.streamVal}.map($keyAssignerMapFunctionVal)
         |""".strip

    outputs.appendMain(codeBlock)

    val mappedStream = GeneratedUnkeyedDataStream(streamIdentifier + "_with_key", mappedStreamVal, recordType, keyType, isContextual = false)

    this.keyStreamByRecordKey(outputs, mappedStream, streamIdentifier)
  }

  /**
   * Converts a data stream to a keyed stream using the existing record keys.
   */
  def keyStreamByRecordKey(outputs: GeneratorOutputs,
                           stream: GeneratedDataStream,
                           streamIdentifier: String): GeneratedKeyedDataStream = {
    val recordType = stream.recordType
    val keyType = stream.keyType

    val keySelectorVal = outputs.newValName(s"stream_${streamIdentifier}_keySelector_")
    val outputStreamVal = outputs.newValName(s"stream_${streamIdentifier}_keyed_")

    val codeBlock =
      q"""val $keySelectorVal = new ${nameOf[RecordWrapperKeySelector[Any, Product]]}[${recordType.toFlinkTerm}, ${keyType.toTerm}](
         |  ${liftTypeDescriptorToTypeInformation(keyType)})
         |val $outputStreamVal = ${stream.streamVal}.keyBy($keySelectorVal, $keySelectorVal.getKeyType)
         |""".strip

    outputs.appendMain(codeBlock)

    GeneratedKeyedDataStream(streamIdentifier, outputStreamVal, recordType, keyType, stream.isContextual)
  }

  /**
   * Keys a stream if the input stream that it was derived from was also keyed.
   *
   * @param outputs      The generator output collector.
   * @param inputStream  The input stream from with the output stream was derived.
   * @param outputStream The derived stream.
   * @return The output stream keyed by the existing record keys, if the input stream was keyed.
   *         Otherwise the original output stream is returned.
   */
  def keyStreamIfInputIsKeyed(outputs: GeneratorOutputs,
                              inputStream: GeneratedDataStream,
                              outputStream: GeneratedDataStream,
                              streamIdentifier: String): GeneratedDataStream = {
    inputStream match {
      case _: GeneratedKeyedDataStream =>
        this.keyStreamByRecordKey(outputs, outputStream, s"${streamIdentifier}_keyed")

      case _ =>
        outputStream
    }
  }

  /**
   * Gets a [[CodeBlock]] defining a function that takes a record key and a value to append to the key and returns the
   * combined tuple.
   *
   * @param functionName    The name of the function.
   * @param keyType         A key type, which must be a tuple.
   * @param appendedKeyType The type to add to the key.
   * @return A [[CodeBlock]] containing the function definition that performs the append operation.
   */
  def getKeyCombinerFunction(functionName: String,
                             keyType: TypeDescriptor[_],
                             appendedKeyType: TypeDescriptor[_]): CodeBlock = {
    if (!keyType.isTuple) {
      throw new IllegalArgumentException("Key types must be tuples.")
    }

    val keyArg = qc"key"
    val newElement = qc"newElem"
    val combinedType = KeyExtractorUtils.combineKeyTypes(keyType, appendedKeyType)

    val combinedKeyElements =
      List.tabulate(keyType.genericArguments.length)(i => qc"$keyArg._${i + 1}") :+ newElement

    val tupleCreationStatement = typeLifter.getTupleCreationStatement(combinedKeyElements)

    qc"""def ${code(functionName)}($keyArg: ${keyType.toTerm}, $newElement: ${appendedKeyType.toTerm}): ${combinedType.toTerm} = {
        |  $tupleCreationStatement
        |}
        |"""
  }

  /**
   * Removes the last element from the keys of records.
   */
  def removeLastKeyElement(outputs: GeneratorOutputs,
                           inputStream: GeneratedDataStream,
                           streamIdentifier: String): GeneratedUnkeyedDataStream = {
    val inputKeyType = inputStream.keyType

    if (!inputKeyType.isTuple) {
      throw new IllegalArgumentException("Key types must be tuples.")
    }

    val recordType = inputStream.recordType
    val outputKeyType = KeyExtractorUtils.removeLastKeyElement(inputKeyType)

    val truncatedKeyElements = List.tabulate(inputKeyType.genericArguments.length - 1)(i => qc"key._${i + 1}")

    val className = outputs.newClassName(s"MapFunction_${inputStream.streamId}_RemoveLastKeyElement")

    val classDef =
      q"""class $className
         |  extends ${nameOf[ModifyRecordKeyMapFunction[Any, Product, Product]]}[${recordType.toFlinkTerm}, ${inputKeyType.toTerm}, ${outputKeyType.toTerm}](
         |    ${liftTypeDescriptorToTypeInformation(recordType)},
         |    ${liftTypeDescriptorToTypeInformation(outputKeyType)}) {
         |
         |  override def getNewKey(value: ${recordType.toFlinkTerm}, key: ${inputKeyType.toTerm}): ${outputKeyType.toTerm} = {
         |    ${typeLifter.getTupleCreationStatement(truncatedKeyElements)}
         |  }
         |}
         |""".strip

    outputs.addClassDef(classDef)

    val outputStreamId = streamIdentifier + "_key_restored"

    val mapFunctionVal = outputs.newValName(s"stream_${streamIdentifier}_removeLastKeyElementMapFunction_")
    val outputStreamVal = outputs.newStreamValName(outputStreamId)

    val codeBlock =
      q"""val $mapFunctionVal = new $className()
         |val $outputStreamVal = ${inputStream.streamVal}.map($mapFunctionVal)
         |""".strip

    outputs.appendMain(codeBlock)

    GeneratedUnkeyedDataStream(outputStreamId, outputStreamVal, recordType, outputKeyType, inputStream.isContextual)
  }

  private def generateKeyAppenderMapFunction(output: GeneratorOutputs,
                                             streamIdentifier: String,
                                             recordType: TypeDescriptor[_],
                                             inputKeyType: TypeDescriptor[_],
                                             keyFunction: FunctionDef): OperatorInfo = {
    val className = output.newClassName(s"MapFunction_${streamIdentifier}_KeyAssigner_")
    val newKeyType = keyFunction.tpe
    val outputKeyType = KeyExtractorUtils.combineKeyTypes(inputKeyType, newKeyType)

    val getKeyDef = output.scalaGenerator.getScalaFunctionDef("getKey", keyFunction)

    val combineKeysDef = this.getKeyCombinerFunction("combineKeys", inputKeyType, newKeyType)

    val classDef =
      q"""class $className
         |  extends ${nameOf[ModifyRecordKeyMapFunction[Any, Product, Product]]}[${recordType.toFlinkTerm}, ${inputKeyType.toTerm}, ${outputKeyType.toTerm}](
         |    ${liftTypeDescriptorToTypeInformation(recordType)},
         |    ${liftTypeDescriptorToTypeInformation(outputKeyType)}) {
         |
         |  private ${code(getKeyDef).indentTail(1)}
         |
         |  private ${combineKeysDef.indentTail(1)}
         |
         |  override def getNewKey(value: ${recordType.toFlinkTerm}, key: ${inputKeyType.toTerm}): ${outputKeyType.toTerm} = {
         |    val newKey = this.getKey(value)
         |    this.combineKeys(key, newKey)
         |  }
         |}
         |""".strip

    output.addClassDef(classDef)

    OperatorInfo(className, recordType, outputKeyType)
  }
}
