package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.flink.compiler.FlinkCompilationException
import com.amazon.milan.flink.metrics.{MetricFactory, SuffixAppendingMetricFactory}
import com.amazon.milan.flink.{FlinkTypeNames, RuntimeEvaluator, TypeUtil}
import com.amazon.milan.program._
import com.amazon.milan.typeutil.TypeDescriptor
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{ConnectedStreams, DataStream, KeyedStream, SingleOutputStreamOperator}
import org.slf4j.LoggerFactory


/**
 * Utilities for creating connected streams.
 */
object FlinkStreamConnector {
  private val logger = Logger(LoggerFactory.getLogger(getClass))

  val typeName: String = getClass.getTypeName.stripSuffix("$")

  /**
   * Connect two data streams where the join conditions are expressed in a Milan function.
   *
   * @param joinCondition   The [[Filter]] expression containing the join operation and the join condition.
   * @param leftDataStream  The left input stream.
   * @param rightDataStream The right input stream.
   * @return A [[ConnectStreamsResult]] containing the connected streams, and a Milan function expressing the portion
   *         of the join conditions that could not be applied prior to the join logic.
   */
  def keyByAndConnectStreams(joinCondition: Filter,
                             leftDataStream: SingleOutputStreamOperator[_],
                             rightDataStream: SingleOutputStreamOperator[_],
                             metricFactory: MetricFactory): ConnectStreamsResult = {
    val leftTypeName = TypeUtil.getTypeName(leftDataStream.getType)
    val rightTypeName = TypeUtil.getTypeName(rightDataStream.getType)

    this.logger.info(s"Connecting streams of type '$leftTypeName' and '$rightTypeName'.")

    val Filter(JoinExpression(leftInput, rightInput), condition) = joinCondition
    val leftInputType = leftInput.recordType
    val rightInputType = rightInput.recordType

    val transformedCondition =
      ContextualTreeTransformer.transform(condition, List(leftInputType, rightInputType))

    val FunctionDef(List(leftArgName, rightArgName), predicateExpr) = transformedCondition

    // Extract and apply any filter conditions that can be applied to the streams before we connect them.
    val preConditionExtractionResult = JoinPreconditionExtractor.extractJoinPrecondition(predicateExpr)

    val leftFilteredStream =
      this.getFilteredStream(
        leftInputType,
        leftDataStream,
        leftArgName,
        preConditionExtractionResult.extracted,
        new SuffixAppendingMetricFactory(metricFactory, "_left_input_filter"))

    val rightFilteredStream =
      this.getFilteredStream(
        rightInputType,
        rightDataStream,
        rightArgName,
        preConditionExtractionResult.extracted,
        new SuffixAppendingMetricFactory(metricFactory, "_right_input_filter"))

    // Extract out the remaining portion of the join condition expression that will be used to key the streams.
    if (preConditionExtractionResult.remainder.isEmpty) {
      throw new InvalidProgramException("Invalid join condition.")
    }

    val keyExpressionExtractionResult = JoinKeyExpressionExtractor.extractJoinKeyExpression(preConditionExtractionResult.remainder.get)

    val keyExpressionFunction =
      keyExpressionExtractionResult.extracted match {
        case Some(p) => FunctionDef(List(leftArgName, rightArgName), p)
        case None => throw new FlinkCompilationException("Non-keyed joins are not supported.")
      }

    // Key the streams using the extracted key expression.
    val (leftKeyFunction, rightKeyFunction) = KeySelectorExtractor.getKeyTupleFunctions(keyExpressionFunction)

    val leftKeyedStream = FlinkKeyedStreamFactory.keyStreamByFunction(leftInputType, leftFilteredStream, leftKeyFunction)
    val leftKeyTypeName = getKeyTypeName(leftKeyedStream.getKeyType)

    val rightKeyedStream = FlinkKeyedStreamFactory.keyStreamByFunction(rightInputType, rightFilteredStream, rightKeyFunction)
    val rightKeyTypeName = getKeyTypeName(rightKeyedStream.getKeyType)

    if (leftKeyTypeName != rightKeyTypeName) {
      throw new FlinkCompilationException(s"Key types do not match: '$leftKeyTypeName' != '$rightKeyTypeName'.")
    }

    val keyTypeName = leftKeyTypeName

    val eval = RuntimeEvaluator.instance

    val connectedStreams =
      eval.evalFunction[KeyedStream[_, _], KeyedStream[_, _], ConnectedStreams[_, _]](
        "leftDataStream",
        FlinkTypeNames.keyedStream(leftTypeName, keyTypeName),
        "rightDataStream",
        FlinkTypeNames.keyedStream(rightTypeName, keyTypeName),
        s"leftDataStream.connect(rightDataStream)",
        leftKeyedStream,
        rightKeyedStream)

    // Construct the Milan FunctionDef that defines the post-join conditions.
    // They will need to be incorporated into the CoProcessFunction, which is done later.
    val postConditionPredicate = keyExpressionExtractionResult.remainder.map(p =>
      new FunctionDef(List(leftArgName, rightArgName), p))

    ConnectStreamsResult(connectedStreams, postConditionPredicate)
  }

  /**
   * Applies a filter to a stream.
   *
   * @param streamRecordType The type of stream records.
   * @param dataStream       The input datastream.
   * @param streamArgName    The argument name that refers to the input stream in the filter expression.
   * @param filterExpression A filter expression.
   * @return A [[DataStream]] containing the filtered stream.
   */
  private def getFilteredStream(streamRecordType: TypeDescriptor[_],
                                dataStream: SingleOutputStreamOperator[_],
                                streamArgName: String,
                                filterExpression: Option[Tree],
                                metricFactory: MetricFactory): SingleOutputStreamOperator[_] = {
    filterExpression match {
      case Some(filter) =>
        FlinkFilterFunctionFactory.applyFilterPortion(streamRecordType, dataStream, streamArgName, filter, metricFactory)

      case None =>
        dataStream
    }
  }

  private def getKeyTypeName(ty: TypeInformation[_]): String = {
    val typeName = TypeUtil.getTypeName(ty)

    // Convert tuple types to the base type that doesn't have any generic arguments.
    if (typeName.startsWith(FlinkTypeNames.tuple)) {
      FlinkTypeNames.tuple
    }
    else {
      typeName
    }
  }
}


case class ConnectStreamsResult(connectedStreams: ConnectedStreams[_, _], unappliedConditions: Option[FunctionDef])
