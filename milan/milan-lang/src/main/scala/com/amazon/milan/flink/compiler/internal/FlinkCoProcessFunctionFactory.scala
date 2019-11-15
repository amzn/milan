package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.flink.compiler.FlinkCompilationException
import com.amazon.milan.flink.components._
import com.amazon.milan.flink.metrics.MetricFactory
import com.amazon.milan.flink.{FlinkTypeNames, RuntimeEvaluator, TypeUtil}
import com.amazon.milan.program._
import com.amazon.milan.types.{Record, RecordWithLineage}
import com.amazon.milan.typeutil.TypeDescriptor
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.datastream.{ConnectedStreams, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.slf4j.LoggerFactory


object FlinkCoProcessFunctionFactory {
  val typeName: String = getClass.getTypeName.stripSuffix("$")

  private val logger = Logger(LoggerFactory.getLogger(getClass))

  def applyCoProcessFunction(mapExpr: MapNodeExpression,
                             dataStream: ConnectedStreams[_, _],
                             joinPostConditions: Option[FunctionDef],
                             lineageFactory: JoinLineageRecordFactory,
                             metricFactory: MetricFactory): SingleOutputStreamOperator[RecordWithLineage[_]] = {
    val leftTypeName = TypeUtil.getTypeName(dataStream.getType1)
    val rightTypeName = TypeUtil.getTypeName(dataStream.getType2)
    val outputTypeName = mapExpr.getRecordTypeName

    val eval = RuntimeEvaluator.instance

    eval.evalFunction[MapNodeExpression, Option[FunctionDef], ConnectedStreams[_, _], JoinLineageRecordFactory, MetricFactory, SingleOutputStreamOperator[RecordWithLineage[_]]](
      "mapExpr",
      eval.getClassName[MapNodeExpression],
      "joinPostConditions",
      s"Option[${eval.getClassName[FunctionDef]}]",
      "dataStream",
      FlinkTypeNames.connectedStreams(leftTypeName, rightTypeName),
      "lineageFactory",
      eval.getClassName[JoinLineageRecordFactory],
      "metricFactory",
      MetricFactory.typeName,
      s"${this.typeName}.applyCoProcessFunctionImpl[$leftTypeName, $rightTypeName, $outputTypeName](mapExpr, joinPostConditions, dataStream, lineageFactory, metricFactory)",
      mapExpr,
      joinPostConditions,
      dataStream,
      lineageFactory,
      metricFactory)
  }

  def applyCoProcessFunctionImpl[TLeft <: Record, TRight <: Record, TOut <: Record](mapExpr: MapNodeExpression,
                                                                                    joinPostConditions: Option[FunctionDef],
                                                                                    dataStream: ConnectedStreams[TLeft, TRight],
                                                                                    lineageFactory: JoinLineageRecordFactory,
                                                                                    metricFactory: MetricFactory): SingleOutputStreamOperator[RecordWithLineage[TOut]] = {
    val processFunction =
      mapExpr.source match {
        case joinExpr: JoinNodeExpression =>
          getJoinedStreamCoProcessFunction[TLeft, TRight, TOut](
            mapExpr,
            joinExpr,
            joinPostConditions,
            dataStream,
            lineageFactory,
            metricFactory)

        case unsupported =>
          throw new FlinkCompilationException(s"Unsupported input stream type to CoProcessFunction: ${unsupported.expressionType}.")
      }

    val outputTypeInformation = processFunction.asInstanceOf[ResultTypeQueryable[RecordWithLineage[TOut]]].getProducedType

    val outputStream = dataStream.process(processFunction, outputTypeInformation)
    this.applyName(outputStream, mapExpr)
  }

  def getJoinedStreamCoProcessFunction[TLeft <: Record, TRight <: Record, TOut <: Record](mapExpr: MapNodeExpression,
                                                                                          joinExpr: JoinNodeExpression,
                                                                                          joinPostConditions: Option[FunctionDef],
                                                                                          dataStream: ConnectedStreams[TLeft, TRight],
                                                                                          lineageFactory: JoinLineageRecordFactory,
                                                                                          metricFactory: MetricFactory): CoProcessFunction[TLeft, TRight, RecordWithLineage[TOut]] = {
    joinExpr match {
      case leftJoin: LeftJoin =>
        getLeftJoinCoProcessFunction(mapExpr, leftJoin, joinPostConditions, dataStream, lineageFactory, metricFactory)

      case fullJoin: FullJoin =>
        getFullJoinCoProcessFunction(mapExpr, fullJoin, joinPostConditions, dataStream, lineageFactory, metricFactory)

      case unsupported =>
        throw new FlinkCompilationException(s"Unsupported join type for CoProcessFunction: ${unsupported.expressionType}.")
    }
  }

  def getFullJoinCoProcessFunction[TLeft <: Record, TRight <: Record, TOut <: Record](mapExpr: MapNodeExpression,
                                                                                      joinExpr: FullJoin,
                                                                                      joinPostConditions: Option[FunctionDef],
                                                                                      dataStream: ConnectedStreams[TLeft, TRight],
                                                                                      lineageFactory: JoinLineageRecordFactory,
                                                                                      metricFactory: MetricFactory): CoProcessFunction[TLeft, TRight, RecordWithLineage[TOut]] = {
    val leftTypeInformation = dataStream.getType1
    val rightTypeInformation = dataStream.getType2

    val eval = RuntimeEvaluator.instance

    val leftInputType = joinExpr.left.recordType
    val rightInputType = joinExpr.right.recordType

    mapExpr match {
      case mapRecord: MapRecord =>
        val outputTypeInformation = eval.createTypeInformation[TOut](mapExpr.recordType.asInstanceOf[TypeDescriptor[TOut]])
        new FullJoinMapToRecordCoProcessFunction[TLeft, TRight, TOut](
          mapRecord,
          leftInputType,
          rightInputType,
          leftTypeInformation,
          rightTypeInformation,
          outputTypeInformation,
          joinPostConditions,
          lineageFactory,
          metricFactory)

      case mapFields: MapFields =>
        val outputTypeInformation = TupleStreamTypeInformation.createFromFieldDefinitions(mapFields.fields)
        new FullJoinMapToFieldsCoProcessFunction[TLeft, TRight](
          mapFields,
          leftInputType,
          rightInputType,
          leftTypeInformation,
          rightTypeInformation,
          outputTypeInformation,
          joinPostConditions,
          lineageFactory,
          metricFactory).asInstanceOf[CoProcessFunction[TLeft, TRight, RecordWithLineage[TOut]]]

      case unsupported =>
        throw new FlinkCompilationException(s"Unsupported map type for full join: ${unsupported.expressionType}.")
    }
  }

  def getLeftJoinCoProcessFunction[TLeft <: Record, TRight <: Record, TOut <: Record](mapExpr: MapNodeExpression,
                                                                                      joinExpr: LeftJoin,
                                                                                      joinPostConditions: Option[FunctionDef],
                                                                                      dataStream: ConnectedStreams[TLeft, TRight],
                                                                                      lineageFactory: JoinLineageRecordFactory,
                                                                                      metricFactory: MetricFactory): CoProcessFunction[TLeft, TRight, RecordWithLineage[TOut]] = {
    val rightTypeInformation = dataStream.getType2
    val eval = RuntimeEvaluator.instance
    val outputTypeInformation = eval.createTypeInformation[TOut](mapExpr.recordType.asInstanceOf[TypeDescriptor[TOut]])

    val leftInputType = joinExpr.left.recordType
    val rightInputType = joinExpr.right.recordType

    mapExpr match {
      case mapRecord: MapRecord =>
        new LeftJoinMapToRecordCoProcessFunction[TLeft, TRight, TOut](
          mapRecord,
          leftInputType,
          rightInputType,
          rightTypeInformation,
          outputTypeInformation,
          joinPostConditions,
          lineageFactory,
          metricFactory)

      case mapFields: MapFields =>
        val outputTypeInformation = TupleStreamTypeInformation.createFromFieldDefinitions(mapFields.fields)
        new LeftJoinMapToFieldsCoProcessFunction[TLeft, TRight](
          mapFields,
          leftInputType,
          rightInputType,
          rightTypeInformation,
          outputTypeInformation,
          joinPostConditions,
          lineageFactory,
          metricFactory).asInstanceOf[CoProcessFunction[TLeft, TRight, RecordWithLineage[TOut]]]
    }
  }

  /**
   * Applies a name to a [[SingleOutputStreamOperator]] based on the information in the graph node representing the
   * stream.
   *
   * @param dataStream The data stream to apply the name to.
   * @param mapExpr    The graph node representing the select statement.
   * @tparam T The type of objects in the stream.
   * @return The named stream, if a name could be generated based on the graph node. Otherwise the data stream is
   *         returned without a new name applied.
   */
  private def applyName[T](dataStream: SingleOutputStreamOperator[T],
                           mapExpr: MapNodeExpression): SingleOutputStreamOperator[T] = {
    val name =
      mapExpr.source match {
        case LeftJoin(left, right, _) =>
          s"LeftEnrichmentJoin [${left.nodeName}] with [${right.nodeName}] -> [${mapExpr.nodeName}]"

        case FullJoin(left, right, _) =>
          s"FullEnrichmentJoin [${left.nodeName}] with [${right.nodeName}] -> [${mapExpr.nodeName}]"
      }

    dataStream.name(name)
  }
}
