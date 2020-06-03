package com.amazon.milan.flink.generator

import com.amazon.milan.flink.internal.{ContextualTreeTransformer, JoinKeyExpressionExtractor, JoinPreconditionExtractor, KeySelectorExtractor}
import com.amazon.milan.program.{FunctionDef, InvalidProgramException, JoinExpression, Tree, TypeChecker, ValueDef}


trait ConnectedStreamsGenerator extends KeyedStreamGenerator with FilteredStreamGenerator {
  val typeLifter: TypeLifter

  def keyByAndConnectStreams(env: GeneratorOutputs,
                             joinExpr: JoinExpression,
                             leftInput: GeneratedDataStream,
                             rightInput: GeneratedDataStream): GeneratedConnectedStreams = {

    val transformedCondition = ContextualTreeTransformer.transform(joinExpr.condition)

    val FunctionDef(List(ValueDef(leftArgName, _), ValueDef(rightArgName, _)), predicateExpr) = transformedCondition

    // Extract and apply any filter conditions that can be applied to the streams before we connect them.
    val preConditionExtractionResult = JoinPreconditionExtractor.extractJoinPrecondition(predicateExpr)

    val leftFilteredStream =
      this.getFilteredStream(
        env,
        s"${joinExpr.nodeName}_leftInput",
        leftInput,
        leftArgName,
        preConditionExtractionResult.extracted)

    val rightFilteredStream =
      this.getFilteredStream(
        env,
        s"${joinExpr.nodeName}_rightInput",
        rightInput,
        rightArgName,
        preConditionExtractionResult.extracted)

    // Extract out the remaining portion of the join condition expression that will be used to key the streams.
    if (preConditionExtractionResult.remainder.isEmpty) {
      throw new InvalidProgramException("Invalid join condition.")
    }

    val keyExpressionExtractionResult = JoinKeyExpressionExtractor.extractJoinKeyExpression(preConditionExtractionResult.remainder.get)

    val keyExpressionFunction =
      keyExpressionExtractionResult.extracted match {
        case Some(expr) => FunctionDef(List(ValueDef(leftArgName, leftInput.recordType), ValueDef(rightArgName, rightInput.recordType)), expr)
        case None => throw new FlinkGeneratorException("Non-keyed joins are not supported.")
      }

    // Key the streams using the extracted key expression.
    val (leftKeyFunction, rightKeyFunction) = KeySelectorExtractor.getKeyTupleFunctions(keyExpressionFunction)
    TypeChecker.typeCheck(leftKeyFunction)
    TypeChecker.typeCheck(rightKeyFunction)

    if (leftKeyFunction.tpe != rightKeyFunction.tpe) {
      throw new InvalidProgramException("Key types must be equivalent.")
    }

    val leftKeyedStream = this.keyStreamByFunction(env, leftFilteredStream, leftKeyFunction, joinExpr.nodeName + "_left_input")
    val rightKeyedStream = this.keyStreamByFunction(env, rightFilteredStream, rightKeyFunction, joinExpr.nodeName + "_right_input")

    val outputStreamVal = env.newValName(s"connected_${joinExpr.nodeName}_")
    env.appendMain(s"val $outputStreamVal = ${leftKeyedStream.streamVal}.connect(${rightKeyedStream.streamVal})")

    // Construct the Milan FunctionDef that defines the post-join conditions.
    // They will need to be incorporated into the CoProcessFunction, which is done later.
    val postConditionPredicate = keyExpressionExtractionResult.remainder.map(predicateExpr => {
      val f = FunctionDef(List(ValueDef(leftArgName, leftInput.recordType), ValueDef(rightArgName, rightInput.recordType)), predicateExpr)
      TypeChecker.typeCheck(f)
      f
    })

    GeneratedConnectedStreams(
      joinExpr.nodeName,
      outputStreamVal,
      postConditionPredicate,
      leftKeyedStream.keyType,
      leftKeyedStream.recordType,
      rightKeyedStream.recordType,
      isContextual = false)
  }

  private def getFilteredStream(env: GeneratorOutputs,
                                outputIdentifier: String,
                                input: GeneratedDataStream,
                                streamArgName: String,
                                filterExpression: Option[Tree]): GeneratedDataStream = {
    filterExpression match {
      case Some(expr) =>
        this.applyFilterPortion(env, outputIdentifier, input, streamArgName, expr)

      case None =>
        input
    }
  }
}
