package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.flink.components.FilterExpressionFilterFunction
import com.amazon.milan.flink.metrics.MetricFactory
import com.amazon.milan.flink.{FlinkTypeNames, RuntimeEvaluator, TypeUtil}
import com.amazon.milan.program.{Filter, FunctionDef, Tree}
import com.amazon.milan.typeutil.TypeDescriptor
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
import org.slf4j.LoggerFactory


object FlinkFilterFunctionFactory {
  private val logger = Logger(LoggerFactory.getLogger(getClass))
  private val typeName: String = getClass.getTypeName.stripSuffix("$")

  /**
   * Creates and applies a filter operation on a Flink [[DataStream]].
   *
   * @param filterExpr A [[Filter]] expression.
   * @param dataStream The data stream to filter.
   * @return The Flink [[SingleOutputStreamOperator]] that is the result of the filter operation.
   */
  def applyFilter(filterExpr: Filter,
                  dataStream: SingleOutputStreamOperator[_],
                  metricFactory: MetricFactory): SingleOutputStreamOperator[_] = {
    this.applyFilter(filterExpr.recordType, dataStream, filterExpr.predicate, metricFactory)
  }

  /**
   * Creates and applies a filter operation on a Flink [[DataStream]].
   *
   * @param streamRecordType The type of stream records.
   * @param dataStream       The data stream to filter.
   * @param filterFunction   A [[FunctionDef]] containging the filter predicate.
   * @return The Flink [[DataStream]] that is the result of the filter operation.
   */
  def applyFilter(streamRecordType: TypeDescriptor[_],
                  dataStream: SingleOutputStreamOperator[_],
                  filterFunction: FunctionDef,
                  metricFactory: MetricFactory): SingleOutputStreamOperator[_] = {
    val streamTypeName = TypeUtil.getTypeName(dataStream.getType)
    val eval = RuntimeEvaluator.instance

    eval.evalFunction[TypeDescriptor[_], SingleOutputStreamOperator[_], FunctionDef, MetricFactory, SingleOutputStreamOperator[_]](
      "streamType",
      TypeDescriptor.typeName(streamTypeName),
      "dataStream",
      FlinkTypeNames.singleOutputStreamOperator(streamTypeName),
      "filterFunction",
      eval.getClassName[FunctionDef],
      "metricFactory",
      MetricFactory.typeName,
      s"${this.typeName}.applyFilterImpl(streamType, dataStream, filterFunction, metricFactory)",
      streamRecordType,
      dataStream,
      filterFunction,
      metricFactory)
  }

  /**
   * Applies a filter operation using the portion of an expression tree that references the specified argument.
   *
   * @param streamRecordType The type of stream records.
   * @param dataStream       The data stream to filter.
   * @param streamArgName    The name of the argument in the predicate expression that refers to the input stream.
   * @param filterExpression An expression tree that may contain filter conditions for the input stream.
   * @return The Flink [[SingleOutputStreamOperator]] that is the result of the filter operation. If the expression tree does not
   *         contain conditions on the input stream, the input data stream is returned.
   */
  def applyFilterPortion(streamRecordType: TypeDescriptor[_],
                         dataStream: SingleOutputStreamOperator[_],
                         streamArgName: String,
                         filterExpression: Tree,
                         metricFactory: MetricFactory): SingleOutputStreamOperator[_] = {
    TreeArgumentSplitter.splitTree(filterExpression, streamArgName).extracted match {
      case Some(filter) =>
        val streamFilterFunc = new FunctionDef(List(streamArgName), filter)
        this.applyFilter(streamRecordType, dataStream, streamFilterFunc, metricFactory)

      case None =>
        dataStream
    }
  }

  def applyFilterImpl[T](streamRecordType: TypeDescriptor[T],
                         dataStream: SingleOutputStreamOperator[T],
                         filterFunction: FunctionDef,
                         metricFactory: MetricFactory): SingleOutputStreamOperator[T] = {
    val filter = new FilterExpressionFilterFunction[T](streamRecordType, filterFunction, metricFactory)
    dataStream.filter(filter).name(s"Filter [${dataStream.getName}]")
  }
}
