package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.application.metrics.HistogramDefinition
import com.amazon.milan.flink._
import com.amazon.milan.flink.components.HistogramProcessFunction
import com.amazon.milan.program.StreamExpression
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.slf4j.LoggerFactory


/**
 * Factory for compiling user-defined metrics into a Flink environment.
 */
object UserMetricsFactory {
  private val logger = Logger(LoggerFactory.getLogger(getClass))
  private val typeName: String = getClass.getTypeName.stripSuffix("$")

  /**
   * Compiles any histograms associated with a stream into the Flink environment.
   *
   * @param histogramDefinitions Definitions of histograms to add to the stream.
   * @param dataStream           The input data stream.
   * @param streamExpr           The graph node expression that computes the data stream.
   */
  def compileHistograms(histogramDefinitions: List[HistogramDefinition[_]],
                        dataStream: SingleOutputStreamOperator[_],
                        streamExpr: StreamExpression): Unit = {
    if (histogramDefinitions.isEmpty) {
      return
    }

    val streamTypeName = dataStream.getType.getTypeName
    val eval = RuntimeEvaluator.instance

    eval.evalFunction[List[HistogramDefinition[_]], SingleOutputStreamOperator[_], StreamExpression, Unit](
      "histogramDefinitions",
      s"List[com.amazon.milan.application.metrics.HistogramDefinition[_]]",
      "dataStream",
      FlinkTypeNames.singleOutputStreamOperator(streamTypeName),
      "streamExpr",
      eval.getClassName[StreamExpression],
      s"${this.typeName}.compileHistogramsImpl[$streamTypeName](histogramDefinitions, dataStream, streamExpr)",
      histogramDefinitions,
      dataStream,
      streamExpr)
  }

  def compileHistogramsImpl[T](histogramDefinitions: List[HistogramDefinition[_]],
                               dataStream: SingleOutputStreamOperator[T],
                               streamExpr: StreamExpression): Unit = {

    this.logger.info(s"Creating HistogramProcessFunction for stream '${streamExpr.nodeName}' with ID '${streamExpr.nodeId}'.")

    val histogramProcessFunction = new HistogramProcessFunction[T](streamExpr.nodeName, streamExpr.recordType, histogramDefinitions)

    dataStream
      .process(histogramProcessFunction)
      .uid(s"${streamExpr.nodeId}.Histogram")
      .name(s"HistogramProcessFunction for [${streamExpr.nodeName}]")
  }
}
