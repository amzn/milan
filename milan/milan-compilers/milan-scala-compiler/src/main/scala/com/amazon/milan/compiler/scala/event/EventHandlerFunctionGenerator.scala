package com.amazon.milan.compiler.scala.event

import com.amazon.milan.application.DataSink
import com.amazon.milan.application.sinks.{ConsoleDataSink, LogSink, SingletonMemorySink}
import com.amazon.milan.compiler.scala._
import com.amazon.milan.compiler.scala.event.operators.{FullOuterJoin, KeyedSlidingRecordWindowApply, LeftEnrichmentJoin}
import com.amazon.milan.compiler.scala.trees.{JoinKeyExpressionExtractor, JoinPreconditionExtractor, KeySelectorExtractor, TreeArgumentSplitter}
import com.amazon.milan.lang.StateIdentifier
import com.amazon.milan.program.{ConstantValue, ExternalStream, Filter, FlatMap, FullJoin, FunctionDef, GroupBy, InvalidProgramException, JoinExpression, LeftJoin, SlidingRecordWindow, StreamMap, Tree, TypeChecker, ValueDef, WindowApply}
import com.amazon.milan.typeutil.{TypeDescriptor, types}

import scala.language.existentials


class EventHandlerFunctionGenerator(val typeLifter: TypeLifter)
  extends ScalarFunctionGenerator(typeLifter.typeEmitter, new IdentityTreeTransformer)
    with EventHandlerAggregateFunctionGenerator
    with ConsumerGenerator
    with StateInterfaceGenerator
    with ScanOperationGenerator {

  import typeLifter._

  /**
   * Generates the implementation of a [[Filter]] expression.
   */
  def generateFilter(outputs: GeneratorOutputs,
                     inputStream: StreamInfo,
                     filterExpr: Filter): StreamInfo = {

    val recordArg = ValName("record")

    val filterFunctionName = CodeBlock(toValidName(s"filter_${filterExpr.nodeName}"))
    val filterMethodDef = outputs.scalaGenerator.getScalaFunctionDef(filterFunctionName.value, filterExpr.predicate)

    outputs.addMethod("private " + filterMethodDef)

    val collectorMethod = outputs.getCollectorName(filterExpr)

    val methodBody =
      qc"""
          |if ($filterFunctionName($recordArg.value)) {
          |  $collectorMethod($recordArg)
          |}
          |"""

    val consumerInfo = StreamConsumerInfo(filterExpr.nodeName, "input")

    this.generateConsumer(outputs, inputStream, consumerInfo, recordArg, methodBody)

    inputStream.withExpression(filterExpr)
  }

  /**
   * Generates the implementation of a [[StreamMap]] whose input is a standard data stream or a grouping.
   * Any existing record keys will be retained in the output records.
   */
  def generateStreamMap(outputs: GeneratorOutputs,
                        inputStream: StreamInfo,
                        mapExpr: StreamMap): StreamInfo = {
    val mapFunction = mapExpr.expr
    val mapFunctionName = CodeBlock(toValidName(s"mapfunction_${mapExpr.nodeName}"))
    val mapFunctionDef = outputs.scalaGenerator.getScalaFunctionDef(mapFunctionName.value, mapFunction)

    outputs.addMethod("private " + mapFunctionDef)

    val collectorMethod = outputs.getCollectorName(mapExpr)

    val recordArg = ValName("record")
    val methodBody =
      qc"""
          |val mappedValue = $mapFunctionName($recordArg.value)
          |val outputRecord = $recordArg.withValue(mappedValue)
          |$collectorMethod(outputRecord)
          |"""

    val consumerInfo = StreamConsumerInfo(mapExpr.nodeName, "input")

    this.generateConsumer(outputs, inputStream, consumerInfo, recordArg, methodBody)

    inputStream.withExpression(mapExpr)
  }

  /**
   * Generates the implementation of a [[StreamMap]] whose input is a [[JoinExpression]].
   */
  def generateJoinSelect(outputs: GeneratorOutputs,
                         inputJoinedStream: StreamInfo,
                         mapExpr: StreamMap): StreamInfo = {
    // The input stream should be producing tuples of records, which we need to split to send into the map expression.
    // First we'll generate the map function (a function of two arguments), then we'll generate a consumer method that
    // extracts those arguments from the tuple in the input record.

    val mapFunctionName = i"mapFunction_${mapExpr.nodeName}"
    val mapFunctionDef = outputs.scalaGenerator.getScalaFunctionDef(mapFunctionName.value, mapExpr.expr)
    outputs.addMethod(mapFunctionDef)

    val collectorMethod = outputs.getCollectorName(mapExpr)

    val recordArg = ValName("joinedRecords")
    val consumerBody =
      qc"""val (leftRecord, rightRecord) = $recordArg.value
          |val mappedRecord = this.$mapFunctionName(leftRecord, rightRecord)
          |val wrapped = ${nameOf[RecordWrapper[Any, Product]]}.wrap(mappedRecord)
          |$collectorMethod(wrapped)
          |"""

    val consumerInfo = StreamConsumerInfo(mapExpr.nodeName, "input")
    this.generateConsumer(outputs, inputJoinedStream, consumerInfo, recordArg, consumerBody)

    StreamInfo(mapExpr, inputJoinedStream.contextKeyType, types.EmptyTuple)
  }

  /**
   * Generates a data sink method and adds it to the generated class.
   */
  def generateDataSink(context: GeneratorContext,
                       stream: StreamInfo,
                       sink: DataSink[_]): StreamConsumerInfo = {
    // First try the plugin, then fall back to the known data sinks.
    context.plugin.generateDataSink(context, stream, sink) match {
      case Some(consumer) =>
        consumer

      case None =>
        sink match {
          case consoleSink: ConsoleDataSink[_] =>
            this.generateConsoleDataSink(context.outputs, stream, consoleSink)

          case logSink: LogSink[_] =>
            this.generateLogSink(context.outputs, stream, logSink)

          case memorySink: SingletonMemorySink[_] =>
            this.generateMemorySink(context.outputs, stream, memorySink)
        }
    }
  }

  /**
   * Generates a collector method, which takes records from a stream and sends them to all consumers of that stream.
   *
   * @param outputs   The generator output collector.
   * @param provider  The provider stream.
   * @param consumers A list of consumers of the provider stream.
   * @return The name of the generated collector method.
   */
  def generateCollector(outputs: GeneratorOutputs,
                        provider: StreamInfo,
                        consumers: List[StreamConsumerInfo]): MethodName = {
    val collectorName = outputs.getCollectorName(provider.expr)

    if (!outputs.collectorExists(collectorName.value)) {
      val recordArg = ValName("record")
      val argDef = qc"$recordArg: ${this.getRecordWrapperTypeName(provider)}"

      val consumerCalls =
        consumers
          .map(consumer => s"${outputs.getConsumerName(consumer)}($recordArg)")
          .mkString("\n")

      val collectorDef =
        q"""private def $collectorName($argDef): Unit = {
           |  ${code(consumerCalls).indentTail(1)}
           |}
           |""".codeStrip

      outputs.addCollectorMethod(collectorName.value, collectorDef)
    }

    collectorName
  }

  /**
   * Generates the implementation of an external stream.
   * External streams become a public consume_{name} method on the class, and a case block in the general consume()
   * method.
   */
  def generateExternalStream(outputs: GeneratorOutputs,
                             stream: ExternalStream): StreamInfo = {
    val methodName = MethodName(toValidName(s"consume${stream.nodeName}"))

    val recordType = stream.recordType
    val collectorMethod = outputs.getCollectorName(stream)

    val methodDef =
      q"""def $methodName(value: ${recordType.toTerm}): Unit = {
         |  val record = ${nameOf[RecordWrapper[Any, Any]]}.wrap(value)
         |  $collectorMethod(record)
         |}
         |""".codeStrip

    outputs.addMethod(methodDef)
    outputs.addExternalStream(stream.nodeId, stream.nodeName, methodName, stream.recordType)

    StreamInfo(stream, types.EmptyTuple, types.EmptyTuple)
  }

  /**
   * Generates the implementation of a [[JoinExpression]].
   */
  def generateJoin(context: GeneratorContext,
                   leftInputStream: StreamInfo,
                   rightInputStream: StreamInfo,
                   joinExpr: JoinExpression): StreamInfo = {
    joinExpr match {
      case leftJoin: LeftJoin =>
        this.generateLeftJoin(context, leftInputStream, rightInputStream, leftJoin)

      case fullJoin: FullJoin =>
        this.generateFullJoin(context, leftInputStream, rightInputStream, fullJoin)
    }
  }

  /**
   * Generates the implementation of a [[FullJoin]].
   */
  def generateFullJoin(context: GeneratorContext,
                       leftInputStream: StreamInfo,
                       rightInputStream: StreamInfo,
                       joinExpr: FullJoin): StreamInfo = {
    // Add a field to the generated class that implements FullOuterJoin for this operation.
    val joinerField = this.generateFullOuterJoinClass(context, leftInputStream, rightInputStream, joinExpr)

    // Add the methods that consume the left and right records and send the pairs of joined records onwards.
    this.generateJoinLeftConsumer(context.outputs, leftInputStream, joinerField, joinExpr)
    this.generateJoinRightConsumer(context.outputs, rightInputStream, joinerField, joinExpr)

    StreamInfo(joinExpr, leftInputStream.contextKeyType, leftInputStream.keyType)
  }

  /**
   * Generates the implementation of a [[LeftJoin]].
   */
  def generateLeftJoin(context: GeneratorContext,
                       leftInputStream: StreamInfo,
                       rightInputStream: StreamInfo,
                       joinExpr: LeftJoin): StreamInfo = {
    // Add a field to the generated class that implements LeftEnrichmentJoin for this operation.
    val joinerField = this.generateLeftEnrichmentJoinClass(context, leftInputStream, rightInputStream, joinExpr)

    // Add the methods that consume the left and right records and send the pairs of joined records onwards.
    this.generateJoinLeftConsumer(context.outputs, leftInputStream, joinerField, joinExpr)
    this.generateJoinRightConsumer(context.outputs, rightInputStream, joinerField, joinExpr)

    StreamInfo(joinExpr, leftInputStream.contextKeyType, leftInputStream.keyType)
  }

  /**
   * Generates the implementation of a [[GroupBy]] expression.
   */
  def generateGroupBy(context: GeneratorContext,
                      inputStream: StreamInfo,
                      groupExpr: GroupBy): StreamInfo = {
    val keyFunction = groupExpr.expr

    val setKeyMethodName = toValidName(s"setRecordKey_${groupExpr.nodeName}")
    val keyedStream = this.generateSetRecordKeyMethod(context.outputs, inputStream, setKeyMethodName, keyFunction)

    val consumerInfo = StreamConsumerInfo(groupExpr.nodeName, "input")
    val collectorName = context.outputs.getCollectorName(groupExpr)
    val recordArg = ValName("record")
    val methodBody = qc"$collectorName(${code(setKeyMethodName)}($recordArg))"
    this.generateConsumer(context.outputs, inputStream, consumerInfo, recordArg, methodBody)

    keyedStream.withExpression(groupExpr)
  }

  /**
   * Generates the implementation of a [[FlatMap]] expression where the input is a grouped stream.
   */
  def generateFlatMapOfGroupedStream(context: GeneratorContext,
                                     inputStream: StreamInfo,
                                     flatMapExpr: FlatMap): StreamInfo = {
    // FlatMap of a grouped stream (either keyed or windowed) can be done by just applying the operations in the mapping
    // function to the input stream.

    // The map expression in the FlatMap is a stream expression, so we can ask the context to generate that stream.
    // First we need to tell the context to map the argument name in the FlatMap function to the generated stream that
    // is the input to the FlatMap. Any operation using that stream by referencing the function argument should by using
    // the "keyed" version of the stream.

    val inputStreamTerm = inputStream.addKeyToContext()
    val streamArg = flatMapExpr.expr.arguments.find(_.tpe.isStream).get
    val flatMapContext = context.withStreamTerm(streamArg.name, inputStreamTerm)

    val mappedStream = EventHandlerClassGenerator.getOrGenerateStream(flatMapContext, flatMapExpr.expr.body)

    // We need to generate the consumer method for the FlatMap, which will strip the key off the records coming from the
    // Map we generated above. FlatMap removes the current grouping.
    val recordArg = ValName("record")

    val fullKeyType = mappedStream.fullKeyType
    val outputKeyType = inputStream.contextKeyType
    val getOutputKeyCode = this.generateGetContextKeyCode(ValName("key"), fullKeyType, outputKeyType)

    val consumerInfo = StreamConsumerInfo(flatMapExpr.nodeName, "input")
    val collectorMethod = context.outputs.getCollectorName(flatMapExpr)

    val consumerBody =
      qc"""
          |def getOutputKey(key: ${fullKeyType.toTerm}): ${outputKeyType.toTerm} = {
          |  ${getOutputKeyCode.indentTail(1)}
          |}
          |val outputKey = getOutputKey($recordArg.key)
          |$collectorMethod($recordArg.withKey(outputKey))
          |"""

    this.generateConsumer(context.outputs, mappedStream, consumerInfo, recordArg, consumerBody)

    // The record type of the output stream are the same as the mapped stream from above, but with the key removed.
    mappedStream.withExpression(flatMapExpr).withContextKeyType(inputStream.contextKeyType).withKeyType(types.EmptyTuple)
  }

  /**
   * Generates the implementation of a [[WindowApply]] expression where the input is a sliding record window.
   */
  def generateApplyRecordWindowOfKeyedStream(context: GeneratorContext,
                                             inputStream: StreamInfo,
                                             applyExpr: WindowApply,
                                             sourceWindow: SlidingRecordWindow): StreamInfo = {
    val applyClassDef = this.generateKeyedSlidingWindowApplyClass(context, inputStream, applyExpr, sourceWindow)
    val applyField = ValName(context.outputs.newFieldName(s"slidingRecordWindowApply_${applyExpr.nodeName}_"))
    val applyFieldDef = qc"private val $applyField = new ${applyClassDef.indentTail(1)}"

    val collector = context.outputs.getCollectorName(applyExpr)

    context.outputs.addField(applyFieldDef.value)

    val recordArg = ValName("record")
    val methodBody =
      qc"""
          |val output = $applyField.processRecord($recordArg)
          |$collector(output)
          |"""

    val consumerInfo = StreamConsumerInfo(applyExpr.nodeName, "input")

    this.generateConsumer(context.outputs, inputStream, consumerInfo, recordArg, methodBody)

    inputStream.withExpression(applyExpr).withKeyType(types.EmptyTuple)
  }

  /**
   * Generates a method that replaced the key of a [[RecordWrapper]] with a new key using a key function that is applied
   * to the record values.
   *
   * @param outputs     The generator output collector.
   * @param inputStream The input stream being consumed.
   * @param methodName  The name of the method to generate.
   * @param keyFunction A [[FunctionDef]] that computes key values from the record values.
   * @return A [[StreamInfo]] describing the output from the generated method.
   */
  private def generateSetRecordKeyMethod(outputs: GeneratorOutputs,
                                         inputStream: StreamInfo,
                                         methodName: String,
                                         keyFunction: FunctionDef): StreamInfo = {
    val newKeyType = keyFunction.tpe
    val recordArg = ValName("record")

    // Get the function definition for extracting the new key elements from input record values.
    val keyFunctionDef = outputs.scalaGenerator.getScalaFunctionDef("getKey", keyFunction)

    // Get the function definition that returns new record keys.
    val keyCombinerDef = this.getKeyCombinerFunction("getNewRecordKey", inputStream, newKeyType)

    val methodBody =
      qc"""
          |${code(keyFunctionDef)}
          |
          |$keyCombinerDef
          |
          |val newKeyElem = getKey($recordArg.value)
          |val fullKey = getNewRecordKey($recordArg.key, newKeyElem)
          |$recordArg.withKey(fullKey)
          |"""

    val outputStream = inputStream.withKeyType(newKeyType)

    this.generateRecordWrapperMethod(
      outputs,
      inputStream,
      this.getRecordWrapperTypeName(outputStream),
      methodName,
      recordArg,
      methodBody)

    outputStream
  }

  /**
   * Gets a definition of a function that takes the current full record key and a new key element and returns the full
   * key that is produced by combining the two.
   * Any elements of the input key that is contextual will be retained, while any non-contextual element will be
   * replaced by the new key element.
   */
  private def getKeyCombinerFunction(methodName: String,
                                     inputStream: StreamInfo,
                                     newKeyType: TypeDescriptor[_]): CodeBlock = {
    val inputKeyType = inputStream.fullKeyType
    val contextKeyType = inputStream.contextKeyType

    if (contextKeyType.genericArguments.isEmpty) {
      // No context key means we completely replace any existing key with the new key.
      val outputKeyType = TypeDescriptor.createTuple[Product](List(newKeyType))

      qc"""def ${code(methodName)}(currentKey: ${inputKeyType.toTerm}, newKey: ${newKeyType.toTerm}): ${outputKeyType.toTerm} = {
          |  Tuple1(newKey)
          |}
          |"""
    }
    else {
      // The input stream has contextual and possibly non-contextual key elements.
      // We want to copy the contextual elements into the new key and then append the new key element.

      val outputKeyType = TypeDescriptor.augmentTuple(contextKeyType, newKeyType)
      val currentKey = qc"currentKey"
      val newElem = qc"newElem"

      val combinedKeyElements = List.tabulate(contextKeyType.genericArguments.length)(i => qc"$currentKey._${i + 1}") :+ newElem
      val tupleCreationStatement = getTupleCreationStatement(combinedKeyElements)
      qc"""def ${code(methodName)}(currentKey: ${inputKeyType.toTerm}, $newElem: ${newKeyType.toTerm}): ${outputKeyType.toTerm} = {
          |  $tupleCreationStatement
          |}
          |"""
    }
  }

  /**
   * Generates a data sink that prints records to the console using their toString method.
   */
  private def generateConsoleDataSink(outputs: GeneratorOutputs,
                                      stream: StreamInfo,
                                      sink: ConsoleDataSink[_]): StreamConsumerInfo = {
    val recordArg = ValName("record")
    val methodBody = qc"println($recordArg.value)"
    val consumerIdentifier = outputs.newConsumerIdentifier(s"${stream.streamId}_sink")
    val consumerInfo = StreamConsumerInfo(consumerIdentifier, "input")
    this.generateConsumer(outputs, stream, consumerInfo, recordArg, methodBody)
    consumerInfo
  }

  /**
   * Generates a data sink that prints records to the logger using their toString method.
   */
  private def generateLogSink(outputs: GeneratorOutputs,
                              stream: StreamInfo,
                              sink: LogSink[_]): StreamConsumerInfo = {
    val recordArg = ValName("record")
    val methodBody = qc"this.${outputs.loggerField}.info($recordArg.value.toString)"
    val consumerIdentifier = outputs.newConsumerIdentifier(s"${stream.streamId}_sink")
    val consumerInfo = StreamConsumerInfo(consumerIdentifier, "input")
    this.generateConsumer(outputs, stream, consumerInfo, recordArg, methodBody)
    consumerInfo
  }

  /**
   * Generates a data sink that adds records to a [[SingletonMemorySink]] sink.
   */
  private def generateMemorySink(outputs: GeneratorOutputs,
                                 stream: StreamInfo,
                                 sink: SingletonMemorySink[_]): StreamConsumerInfo = {
    val recordArg = ValName("record")
    val methodBody = qc"${nameOf[SingletonMemorySink[Any]]}.add(${sink.sinkId}, $recordArg.value)"
    val consumerIdentifier = outputs.newConsumerIdentifier(s"${stream.streamId}_sink")
    val consumerInfo = StreamConsumerInfo(consumerIdentifier, "input")
    this.generateConsumer(outputs, stream, consumerInfo, recordArg, methodBody)
    consumerInfo
  }

  /**
   * Generates a class that implements [[FullOuterJoin]] for a [[JoinExpression]] and stores an instance of that
   * class in a class field.
   *
   * @return A [[ValName]] containing the name of the class field where the generated class instance is assigned.
   */
  private def generateFullOuterJoinClass(context: GeneratorContext,
                                         leftInputStream: StreamInfo,
                                         rightInputStream: StreamInfo,
                                         joinExpr: JoinExpression): ValName = {
    val FunctionDef(List(ValueDef(leftArgName, _), ValueDef(rightArgName, _)), joinConditionBody) = joinExpr.condition
    val leftArg = ValueDef(leftArgName, leftInputStream.recordType)
    val rightArg = ValueDef(rightArgName, rightInputStream.recordType)

    // Extract the portion of the join condition that can be applied as a pre-filter on the input streams.
    val preConditionExtractionResult = JoinPreconditionExtractor.extractJoinPrecondition(joinConditionBody)
    val keyExtractionResultOption = preConditionExtractionResult.remainder.map(JoinKeyExpressionExtractor.extractJoinKeyExpression)

    val keyExtractionResult =
      keyExtractionResultOption match {
        case Some(result) if result.extracted.isDefined => result
        case None => throw new InvalidProgramException("Join key could not be determined.")
      }

    // Create the functions that compute join keys from the input records.
    val keyExpressionFunction = FunctionDef(List(leftArg, rightArg), keyExtractionResult.extracted.get)
    val (leftKeySelector, rightKeySelector) = KeySelectorExtractor.getKeyTupleFunctions(keyExpressionFunction)

    val keyType = leftKeySelector.tpe
    val leftRecordType = leftInputStream.recordType
    val rightRecordType = rightInputStream.recordType

    // Create the keyed state interface used to store the right record for each join key.
    val rightState = this.generateKeyedStateInterface(context, joinExpr, StateIdentifier.JOIN_RIGHT_STATE, keyType, rightRecordType)
    val leftSTate = this.generateKeyedStateInterface(context, joinExpr, StateIdentifier.JOIN_LEFT_STATE, keyType, leftRecordType)

    // Get the functions that apply pre-filters to the input streams.
    val (leftPreCondition, rightPrecondition) =
      this.getPreConditionFunctions(joinExpr.condition, preConditionExtractionResult.extracted)

    // Get the function that applies a post-filter to pairs of joined records.
    val postConditionFunction = this.getPostConditionFunction(joinExpr.condition, keyExtractionResult.remainder)

    val joinerVal = ValName(s"joiner_${toValidName(joinExpr.nodeName)}")

    val outputs = context.outputs

    val joinerType = qc"${nameOf[FullOuterJoin[Any, Any, Any, Any, Any]]}[${leftInputStream.recordType.toTerm}, ${leftInputStream.fullKeyType.toTerm}, ${rightInputStream.recordType.toTerm}, ${rightInputStream.fullKeyType.toTerm}, ${keyType.toTerm}]"

    val fieldDef =
      q"""private val $joinerVal: $joinerType = new $joinerType {
         |  protected override val rightState = ${rightState.indentTail(1)}
         |
         |  protected override val leftState = ${leftSTate.indentTail(1)}
         |
         |  protected override ${code(outputs.scalaGenerator.getScalaFunctionDef("getLeftJoinKey", leftKeySelector)).indentTail(1)}
         |
         |  protected override ${code(outputs.scalaGenerator.getScalaFunctionDef("getRightJoinKey", rightKeySelector)).indentTail(1)}
         |
         |  protected override ${code(outputs.scalaGenerator.getScalaFunctionDef("checkLeftPreCondition", leftPreCondition)).indentTail(1)}
         |
         |  protected override ${code(outputs.scalaGenerator.getScalaFunctionDef("checkRightPreCondition", rightPrecondition)).indentTail(1)}
         |
         |  protected override ${code(outputs.scalaGenerator.getScalaFunctionDef("checkPostCondition", postConditionFunction)).indentTail(1)}
         |}
         |""".codeStrip

    outputs.addField(fieldDef)

    joinerVal
  }

  /**
   * Generates a class that implements [[LeftEnrichmentJoin]] for a [[JoinExpression]] and stores an instance of that
   * class in a class field.
   *
   * @return A [[ValName]] containing the name of the class field where the generated class instance is assigned.
   */
  private def generateLeftEnrichmentJoinClass(context: GeneratorContext,
                                              leftInputStream: StreamInfo,
                                              rightInputStream: StreamInfo,
                                              joinExpr: JoinExpression): ValName = {
    val FunctionDef(List(ValueDef(leftArgName, _), ValueDef(rightArgName, _)), joinConditionBody) = joinExpr.condition
    val leftArg = ValueDef(leftArgName, leftInputStream.recordType)
    val rightArg = ValueDef(rightArgName, rightInputStream.recordType)

    // Extract the portion of the join condition that can be applied as a pre-filter on the input streams.
    val preConditionExtractionResult = JoinPreconditionExtractor.extractJoinPrecondition(joinConditionBody)
    val keyExtractionResultOption = preConditionExtractionResult.remainder.map(JoinKeyExpressionExtractor.extractJoinKeyExpression)

    val keyExtractionResult =
      keyExtractionResultOption match {
        case Some(result) if result.extracted.isDefined => result
        case None => throw new InvalidProgramException("Join key could not be determined.")
      }

    // Create the functions that compute join keys from the input records.
    val keyExpressionFunction = FunctionDef(List(leftArg, rightArg), keyExtractionResult.extracted.get)
    val (leftKeySelector, rightKeySelector) = KeySelectorExtractor.getKeyTupleFunctions(keyExpressionFunction)

    val keyType = leftKeySelector.tpe
    val rightRecordType = rightInputStream.recordType

    // Create the keyed state interface used to store the right record for each join key.
    val rightState = this.generateKeyedStateInterface(context, joinExpr, StateIdentifier.JOIN_RIGHT_STATE, keyType, rightRecordType)

    // Get the functions that apply pre-filters to the input streams.
    val (leftPreCondition, rightPrecondition) =
      this.getPreConditionFunctions(joinExpr.condition, preConditionExtractionResult.extracted)

    // Get the function that applies a post-filter to pairs of joined records.
    val postConditionFunction = this.getPostConditionFunction(joinExpr.condition, keyExtractionResult.remainder)

    val joinerVal = ValName(s"joiner_${toValidName(joinExpr.nodeName)}")

    val outputs = context.outputs

    val joinerType = qc"${nameOf[LeftEnrichmentJoin[Any, Any, Any, Any, Any]]}[${leftInputStream.recordType.toTerm}, ${leftInputStream.fullKeyType.toTerm}, ${rightInputStream.recordType.toTerm}, ${rightInputStream.fullKeyType.toTerm}, ${keyType.toTerm}]"

    val fieldDef =
      q"""private val $joinerVal: $joinerType = new $joinerType {
         |  protected override val rightState = ${rightState.indentTail(1)}
         |
         |  protected override ${code(outputs.scalaGenerator.getScalaFunctionDef("getLeftJoinKey", leftKeySelector)).indentTail(1)}
         |
         |  protected override ${code(outputs.scalaGenerator.getScalaFunctionDef("getRightJoinKey", rightKeySelector)).indentTail(1)}
         |
         |  protected override ${code(outputs.scalaGenerator.getScalaFunctionDef("checkLeftPreCondition", leftPreCondition)).indentTail(1)}
         |
         |  protected override ${code(outputs.scalaGenerator.getScalaFunctionDef("checkRightPreCondition", rightPrecondition)).indentTail(1)}
         |
         |  protected override ${code(outputs.scalaGenerator.getScalaFunctionDef("checkPostCondition", postConditionFunction)).indentTail(1)}
         |}
         |""".codeStrip

    outputs.addField(fieldDef)

    joinerVal
  }

  /**
   * Generates a class method that consumes records from the left input stream of a join.
   */
  private def generateJoinLeftConsumer(outputs: GeneratorOutputs,
                                       leftInputStream: StreamInfo,
                                       joinerName: ValName,
                                       joinExpr: JoinExpression): Unit = {
    val recordArg = ValName("leftRecord")
    val collectorMethod = outputs.getCollectorName(joinExpr)
    val methodBody =
      qc"""this.$joinerName.processLeftRecord($recordArg) match {
          |  case Some(output) => $collectorMethod(output)
          |  case None => ()
          |}
          |"""
    val consumerInfo = StreamConsumerInfo(joinExpr.nodeName, "left")
    this.generateConsumer(outputs, leftInputStream, consumerInfo, recordArg, methodBody)
  }

  /**
   * Generates a class method that consumes records from the right input stream of a join.
   */
  private def generateJoinRightConsumer(outputs: GeneratorOutputs,
                                        rightInputStream: StreamInfo,
                                        joinerName: ValName,
                                        joinExpr: JoinExpression): Unit = {
    val recordArg = ValName("rightRecord")
    val collectorMethod = outputs.getCollectorName(joinExpr)
    val methodBody =
      qc"""this.$joinerName.processRightRecord($recordArg) match {
          |  case Some(output) => $collectorMethod(output)
          |  case None => ()
          |}
          |"""
    val consumerInfo = StreamConsumerInfo(joinExpr.nodeName, "right")
    this.generateConsumer(outputs, rightInputStream, consumerInfo, recordArg, methodBody)
  }

  /**
   * Generates the definition of a class that implements a [[WindowApply]] that is applied to a sliding record window.
   */
  private def generateKeyedSlidingWindowApplyClass(context: GeneratorContext,
                                                   inputStream: StreamInfo,
                                                   applyExpr: WindowApply,
                                                   windowExpr: SlidingRecordWindow): CodeBlock = {
    val inputType = inputStream.recordType
    val fullKeyType = inputStream.fullKeyType
    val contextKeyType = inputStream.contextKeyType
    val keyType = inputStream.keyType
    val outputType = applyExpr.recordType

    val windowSize = windowExpr.windowSize

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

    val applyDef = this.getScalaFunctionDef("apply", applyExpr.expr)

    val windowStateType = TypeDescriptor.ofMap(keyType, TypeDescriptor.ofList(inputType))
    val windowStateDef = this.generateKeyedStateInterface(context, applyExpr, StateIdentifier.STREAM_STATE, stateKeyType, windowStateType)

    qc"""
        |${nameOf[KeyedSlidingRecordWindowApply[Any, Any, Any, Any, Any, Any]]}[${inputType.toTerm}, ${fullKeyType.toTerm}, ${stateKeyType.toTerm}, ${keyType.toTerm}, ${outputType.toTerm}, ${contextKeyType.toTerm}]($windowSize) {
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
        |    this.apply(items)
        |
        |  private ${code(applyDef).indentTail(2)}
        |}
        |"""
  }

  /**
   * Generates function definitions that implement pre-filters to apply to the input streams to a join.
   *
   * @param joinCondition       The full join condition function.
   * @param preConditionPortion The portion of the join condition function that defines the pre-filters.
   * @return A tuple of the left and right pre-filter functions.
   */
  private def getPreConditionFunctions(joinCondition: FunctionDef, preConditionPortion: Option[Tree]): (FunctionDef, FunctionDef) = {
    val FunctionDef(List(leftArg, rightArg), _) = joinCondition

    val leftPreConditionFunction =
      preConditionPortion match {
        case Some(preCondition) =>
          TreeArgumentSplitter.splitTree(preCondition, leftArg.name).extracted match {
            case Some(leftCondition) => FunctionDef(List(leftArg), leftCondition)
            case None => FunctionDef(List(leftArg), ConstantValue(true, types.Boolean))
          }
        case None => FunctionDef(List(leftArg), ConstantValue(true, types.Boolean))
      }

    val rightPreConditionFunction =
      preConditionPortion match {
        case Some(preCondition) =>
          TreeArgumentSplitter.splitTree(preCondition, rightArg.name).extracted match {
            case Some(rightCondition) => FunctionDef(List(rightArg), rightCondition)
            case None => FunctionDef(List(rightArg), ConstantValue(true, types.Boolean))
          }
        case None => FunctionDef(List(rightArg), ConstantValue(true, types.Boolean))
      }

    TypeChecker.typeCheck(leftPreConditionFunction)
    TypeChecker.typeCheck(rightPreConditionFunction)

    (leftPreConditionFunction, rightPreConditionFunction)
  }

  /**
   * Gets a function definition that implements a filter to apply to a pair of joined records.
   *
   * @param joinCondition        The full join condition.
   * @param postConditionPortion The portion of the join condition function body that defines the filter.
   * @return A [[FunctionDef]] that defines the filter function, which takes two arguments (the joined left and right
   *         records) and returns a boolean.
   */
  private def getPostConditionFunction(joinCondition: FunctionDef, postConditionPortion: Option[Tree]): FunctionDef = {
    val FunctionDef(List(leftArg, rightArg), _) = joinCondition

    val postConditionFunction =
      postConditionPortion match {
        case Some(postCondition) =>
          FunctionDef(List(leftArg, rightArg), postCondition)

        case None =>
          FunctionDef(List(leftArg, rightArg), ConstantValue(true, types.Boolean))
      }

    TypeChecker.typeCheck(postConditionFunction)

    postConditionFunction
  }
}
