package com.amazon.milan.flink.generator

import com.amazon.milan.flink.runtime.{IdentityFlatMapFunction, TimeWindowFlatMapProcessWindowFunction}
import com.amazon.milan.program.{FlatMap, Last, LeftWindowedJoin, SingleInputStreamExpression, StreamExpression, Tree}
import com.amazon.milan.typeutil.types


trait FlatMapGenerator
  extends MapFunctionGenerator
    with AggregateFunctionGenerator
    with ProcessWindowFunctionGenerator
    with LastByGenerator
    with ScanOperationGenerator {

  val typeLifter: TypeLifter

  import typeLifter._

  def applyFlatMap(context: GeneratorContext,
                   inputGroupedStream: GeneratedGroupedStream,
                   flatMapExpr: FlatMap): GeneratedUnkeyedDataStream = {
    inputGroupedStream match {
      case dataStream: GeneratedDataStream =>
        this.applyFlatMapToDataSteam(context, dataStream, flatMapExpr)

      case keyedWindowedStream: GeneratedKeyedWindowedStream =>
        this.applyFlatMapToWindowedStream(context, keyedWindowedStream, flatMapExpr)
    }
  }

  /**
   * Applies a FlatMap operation being performed on a left join between a data stream and a windowed stream.
   */
  def applyFlatMap(context: GeneratorContext,
                   leftDataStream: GeneratedDataStream,
                   rightWindowedStream: GeneratedGroupedStream,
                   flatMapExpr: FlatMap,
                   leftWindowedJoinExpr: LeftWindowedJoin): GeneratedUnkeyedDataStream = {
    throw new NotImplementedError()
  }

  private def applyFlatMapToDataSteam(context: GeneratorContext,
                                      inputStream: GeneratedDataStream,
                                      flatMapExpr: FlatMap): GeneratedUnkeyedDataStream = {
    // FlatMap of a grouped stream (either keyed or windowed) can be done by as a Map followed by a FlatMap that uses the identity function.

    // The map expression in the FlatMap is a stream expression, so we can ask the context to generate that stream.
    // First we need to tell the context to map the argument name in the FlatMap function to the generated stream that
    // is the input to the FlatMap.
    val streamArg = flatMapExpr.expr.arguments.find(_.tpe.isStream).get
    val flatMapContext = context.withStreamTerm(streamArg.name, inputStream)
    val mappedStream = flatMapContext.getOrGenerateDataStream(flatMapExpr.expr.body)

    val recordTypeInfoVal = context.output.newValName(s"stream_${flatMapExpr.nodeName}_recordTypeInfo_")
    val keyTypeInfoVal = context.output.newValName(s"stream_${flatMapExpr.nodeName}_keyTypeInfo_")
    val flatMapFunctionVal = context.output.newValName(s"stream_${flatMapExpr.nodeName}_flatMap_")
    val outputStreamVal = context.output.newStreamValName(flatMapExpr)

    val codeBlock =
      q"""
         |val $recordTypeInfoVal = ${liftTypeDescriptorToTypeInformation(mappedStream.recordType)}
         |val $keyTypeInfoVal = ${liftTypeDescriptorToTypeInformation(mappedStream.keyType)}
         |val $flatMapFunctionVal = new ${nameOf[IdentityFlatMapFunction[Any, Product]]}[${mappedStream.recordType.toFlinkTerm}, ${mappedStream.keyType.toTerm}]($recordTypeInfoVal, $keyTypeInfoVal)
         |val $outputStreamVal = ${mappedStream.streamVal}.flatMap($flatMapFunctionVal)
         |""".strip

    context.output.appendMain(codeBlock)

    GeneratedUnkeyedDataStream(flatMapExpr.nodeId, outputStreamVal, mappedStream.recordType, mappedStream.keyType, isContextual = false)
  }

  /**
   * Applies a [[FlatMap]] operation that is performed on a windowed stream.
   *
   * @param context        The generator context.
   * @param windowedStream The input windowed stream.
   * @param flatMapExpr    The [[FlatMap]] expression to apply.
   * @return A [[GeneratedUnkeyedDataStream]] representing the result of the FlatMap operation.
   */
  def applyFlatMapToWindowedStream(context: GeneratorContext,
                                   windowedStream: GeneratedKeyedWindowedStream,
                                   flatMapExpr: FlatMap): GeneratedUnkeyedDataStream = {
    // FlatMap on a windowed stream is tricky.
    // Because we want to use Flink's windowing functionality and not re-implement it, we are limited to the operations
    // allowed on a Flink WindowedStream. This essentially means we can use .aggregate() and that's all.
    // For flatmap functions that easily convert into scan operations, we can compose them all together into a single
    // scan operation and use ScanOperationAggregateFunction.
    // Some Milan operations don't allow this. last() is an example: it requires metadata (Flink watermarks) that aren't
    // available to implementers of Flink's AggregateFunction interface.
    // For that, what we do (for now) is put everything before the last() operation into a ScanOperation and use
    // .aggregate(), with a ProcessWindowFunction that augments the record keys with the window start time, and
    // keys the stream using the augmented keys.
    // We then feed this keyed stream into a KeyedLastByOperator, and feed the output of *that( into the remaining
    // portion of the flatmap function, after the last() operator.
    val streamArg = flatMapExpr.expr.arguments.find(_.tpe.isStream).get

    val expressionParts = this.splitFlatMapFunctionBody(flatMapExpr.expr.body)

    // Stage 1 produces a non-keyed stream of records whose
    val stage1 = this.applyBeforeLastPart(context, windowedStream, flatMapExpr.nodeName + "_stage1", expressionParts.beforeLast)

    val stage2 =
      expressionParts.last match {
        case None =>
          stage1

        case Some(lastExpr) =>
          this.applyLastOperation(context, stage1, lastExpr)
      }

    this.applyAfterLastPart(context, stage2, flatMapExpr.nodeName + "_stage3", expressionParts.afterLast)
  }

  /**
   * Applies the portion of a FlatMap function that comes before a Last expression.
   * Returns a keyed stream where the record key contains the window start time as the final element.
   */
  private def applyBeforeLastPart(context: GeneratorContext,
                                  windowedStream: GeneratedKeyedWindowedStream,
                                  streamIdentifier: String,
                                  exprPart: Option[Tree]): GeneratedKeyedDataStream = {
    exprPart match {
      case None =>
        throw new IllegalArgumentException("Empty expressions are not supported.")

      case Some(expr) =>
        val scanOperation = this.generateScanOperation(context, expr, windowedStream.recordType)

        val aggregateFunctionVal = this.generateScanOperationAggregateFunction(
          context.output,
          streamIdentifier,
          windowedStream.recordType,
          windowedStream.keyType,
          scanOperation)

        val processWindowFunctionClass = this.generateTimeWindowFlatMapProcessWindowFunction(
          context.output,
          windowedStream)

        val optionStreamVal = context.output.newStreamValName(s"${streamIdentifier}_optionRecords")
        val processWindowFunctionVal = context.output.newValName(s"stream_${streamIdentifier}_processWindowFunction_")

        val codeBlock =
          q"""val $processWindowFunctionVal = new ${processWindowFunctionClass.className}()
             |val $optionStreamVal = ${windowedStream.streamVal}.aggregate(
             |  $aggregateFunctionVal,
             |  $processWindowFunctionVal,
             |  $aggregateFunctionVal.getAccumulatorType,
             |  $aggregateFunctionVal.getProducedType,
             |  $processWindowFunctionVal.getProducedType)
             |""".strip

        context.output.appendMain(codeBlock)

        val optionStream = GeneratedUnkeyedDataStream(
          streamIdentifier,
          optionStreamVal,
          processWindowFunctionClass.outputRecordType,
          processWindowFunctionClass.outputKeyType,
          isContextual = windowedStream.isContextual)

        val unpackedStream = this.applyUnpackOptionProcessFunction(context.output, streamIdentifier, optionStream)

        this.keyStreamByRecordKey(context.output, unpackedStream, streamIdentifier)
    }
  }

  /**
   * Applies the portion of a FlatMap function that appears after a Last expression.
   */
  private def applyAfterLastPart(context: GeneratorContext,
                                 inputStream: GeneratedKeyedDataStream,
                                 streamIdentifier: String,
                                 exprPart: Option[StreamExpression]): GeneratedUnkeyedDataStream = {
    // The input to this part is a stream where the record key has the window start time as the last
    // element.
    // We want to:
    // 1. Apply any remaining parts of the expression.
    // 2. Remove the window start time from the records keys.
    val outputKeyedStream = exprPart match {
      case Some(_) =>
        throw new NotImplementedError()

      case None =>
        inputStream
    }

    this.removeLastKeyElement(context.output, outputKeyedStream, streamIdentifier)
  }

  private def generateTimeWindowFlatMapProcessWindowFunction(outputs: GeneratorOutputs,
                                                             windowedStream: GeneratedKeyedWindowedStream): OperatorInfo = {
    val recordType = windowedStream.recordType
    val inputKeyType = windowedStream.keyType
    val outputKeyType = KeyExtractorUtils.combineKeyTypes(inputKeyType, types.Instant)

    // We're windowing an already grouped stream, so we need to append the window key type (java.time.Instance) to the
    // record keys.
    val getCombinedKeyDef = this.getKeyAppenderFunction(
      "addWindowStartTimeToKey",
      inputKeyType,
      types.Instant)

    val className = outputs.newClassName(s"ProcessWindowFunction_${windowedStream.streamId}")
    val classDef =
      q"""class $className
         |  extends ${nameOf[TimeWindowFlatMapProcessWindowFunction[Any, Product, Product]]}[${recordType.toFlinkTerm}, ${inputKeyType.toTerm}, ${outputKeyType.toTerm}](
         |  ${liftTypeDescriptorToTypeInformation(recordType)},
         |  ${liftTypeDescriptorToTypeInformation(outputKeyType)}) {
         |
         |  protected override ${getCombinedKeyDef.indentTail(1)}
         |}
         |""".strip

    outputs.addClassDef(classDef)

    OperatorInfo(className, recordType, outputKeyType)
  }

  /**
   * Splits the body of a flatmap function into three parts: the expression that comes before the first Last expression,
   * the first Last expression, and the expression that comes after the first Last expression.
   * Deeper into the expression tree is "earlier" in execution order.
   *
   * For example, Map(Last(SumBy(...), ...), ...) will be split into (SumBy, Last, Map).
   */
  private def splitFlatMapFunctionBody(expr: Tree): FlatMapExpressionSplit = {
    expr match {
      case last@Last(source) =>
        this.splitFlatMapFunctionBody(source) match {
          case finished@FlatMapExpressionSplit(Some(_), Some(_), Some(_)) =>
            // If we found another Last expression in the recursive call then that's the one we want, because it is
            // before this one.
            finished

          case _ =>
            FlatMapExpressionSplit(Some(last.source), Some(last), None)
        }

      case streamExpr@SingleInputStreamExpression(source) =>
        this.splitFlatMapFunctionBody(source) match {
          case finished@FlatMapExpressionSplit(Some(_), Some(_), Some(_)) =>
            // If we found a Last expression in the recursive call then we don't need to do anything here.
            finished

          case FlatMapExpressionSplit(before, last, None) =>
            // If the source expression was a Last then this is the first expression after the Last.
            FlatMapExpressionSplit(before, last, Some(streamExpr))

          case _ =>
            // How did we get here? Nothing in the recursive call should produce other patterns.
            throw new NotImplementedError()
        }

      case other =>
        FlatMapExpressionSplit(Some(other), None, None)

    }
  }

  case class FlatMapExpressionSplit(beforeLast: Option[Tree], last: Option[Last], afterLast: Option[StreamExpression])

}
