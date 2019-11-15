package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.flink.compiler.FlinkCompilationException
import com.amazon.milan.flink.components._
import com.amazon.milan.flink.metrics.MetricFactory
import com.amazon.milan.flink.types.ArrayRecord
import com.amazon.milan.flink.{FlinkTypeNames, RuntimeEvaluator, TypeUtil}
import com.amazon.milan.program._
import com.amazon.milan.types.{Record, RecordWithLineage}
import com.amazon.milan.typeutil.TypeDescriptor
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.slf4j.LoggerFactory


object FlinkMapFunctionFactory {
  private val logger = Logger(LoggerFactory.getLogger(getClass))
  private val typeName: String = getClass.getTypeName.stripSuffix("$")

  /**
   * Applies a map operation defined in a [[MapNodeExpression]] graph node to a Flink data stream.
   *
   * @param mapExpr         A [[MapNodeExpression]] expressing the map operation.
   * @param inputDataStream The Flink data stream being mapped.
   * @param lineageFactory  A factory for the lineage records that will be produced by the operation.
   * @return A [[SingleOutputStreamOperator]] of the mapped records and their associated lineage records.
   */
  def applyMapFunction(mapExpr: MapNodeExpression,
                       inputDataStream: SingleOutputStreamOperator[_],
                       metricFactory: MetricFactory,
                       lineageFactory: LineageRecordFactory): SingleOutputStreamOperator[RecordWithLineage[_]] = {
    mapExpr match {
      case m: MapRecord =>
        applyMapToRecordFunction(m, inputDataStream, metricFactory, lineageFactory)

      case m: MapFields =>
        applyMapToFieldsFunction(m, inputDataStream, metricFactory, lineageFactory)

      case o =>
        throw new FlinkCompilationException(s"Unsupported MapFunction type '${o.getClass.getSimpleName}'.")
    }
  }

  /**
   * Applies a map operation defined in a [[MapRecord]] graph node to a Flink data stream, where the map operation
   * yields a stream of record objects (not named fields).
   *
   * @param mapExpr         A [[MapRecord]] expressing the map operation.
   * @param inputDataStream The Flink data stream being mapped.
   * @param lineageFactory  A factory for the lineage records that will be produced by the operation.
   * @return A [[SingleOutputStreamOperator]] of the mapped records and their associated lineage records.
   * @note This version of [[applyMapToRecordFunction]] does not require the caller to know the input and output
   *       types of the map.
   */
  def applyMapToRecordFunction(mapExpr: MapRecord,
                               inputDataStream: SingleOutputStreamOperator[_],
                               metricFactory: MetricFactory,
                               lineageFactory: LineageRecordFactory): SingleOutputStreamOperator[RecordWithLineage[_]] = {
    val inputTypeInformation = inputDataStream.getType
    val inputTypeName = TypeUtil.getTypeName(inputTypeInformation)
    val eval = RuntimeEvaluator.instance

    val outputType = mapExpr.recordType
    val outputTypeName = outputType.fullName
    val outputTypeInformation = eval.createTypeInformation(outputType)

    this.logger.info(s"ApplyMapToRecordFunction from $inputTypeInformation to $outputTypeInformation with map function ${mapExpr.expr}")

    eval.evalFunction[MapRecord, SingleOutputStreamOperator[_], MetricFactory, LineageRecordFactory, TypeInformation[_], SingleOutputStreamOperator[RecordWithLineage[_]]](
      "mapExpr",
      eval.getClassName[MapRecord],
      "inputDataStream",
      FlinkTypeNames.singleOutputStreamOperator(inputTypeName),
      "metricFactory",
      MetricFactory.typeName,
      "lineageFactory",
      eval.getClassName[LineageRecordFactory],
      "outputTypeInformation",
      FlinkTypeNames.typeInformation(outputTypeName),
      s"${this.typeName}.applyMapToRecordFunction[$inputTypeName, $outputTypeName](mapExpr, inputDataStream, metricFactory, lineageFactory, outputTypeInformation)",
      mapExpr,
      inputDataStream,
      metricFactory,
      lineageFactory,
      outputTypeInformation)
  }

  /**
   * Applies a map operation defined in a [[MapRecord]] object to a Flink data stream.
   *
   * @param mapExpr         A [[MapRecord]] expressing the map operation.
   * @param inputDataStream The Flink data stream being mapped.
   * @param lineageFactory  A factory for the lineage records that will be produced by the operation.
   * @return A [[SingleOutputStreamOperator]] of the mapped records and their associated lineage records.
   */
  def applyMapToRecordFunction[TIn <: Record, TOut <: Record](mapExpr: MapRecord,
                                                              inputDataStream: SingleOutputStreamOperator[TIn],
                                                              metricFactory: MetricFactory,
                                                              lineageFactory: LineageRecordFactory,
                                                              outputTypeInformation: TypeInformation[TOut]): SingleOutputStreamOperator[RecordWithLineage[TOut]] = {
    val inputTypeName = TypeUtil.getTypeName(inputDataStream.getType)
    val outputTypeName = TypeUtil.getTypeName(outputTypeInformation)
    val eval = RuntimeEvaluator.instance

    val flinkMapFunction =
      eval.evalFunction[MapRecord, TypeDescriptor[_], TypeInformation[TOut], MetricFactory, MapToRecordMapFunction[TIn, TOut]](
        "mapExpr",
        eval.getClassName[MapRecord],
        "inputType",
        TypeDescriptor.typeName("_"),
        "outputTypeInformation",
        FlinkTypeNames.typeInformation(outputTypeName),
        "metricFactory",
        MetricFactory.typeName,
        s"new ${MapToRecordMapFunction.typeName}[$inputTypeName, $outputTypeName](mapExpr, inputType, outputTypeInformation, metricFactory)",
        mapExpr,
        mapExpr.getInputRecordType,
        outputTypeInformation,
        metricFactory)

    val lineageWrapper = new LineageMapFunctionWrapper[TIn, TOut, MapToRecordMapFunction[TIn, TOut]](flinkMapFunction, lineageFactory)

    inputDataStream.map(lineageWrapper).name(this.generateName(mapExpr))
  }

  /**
   * Applies a map operation defined in a [[MapFields]] object to a Flink data stream.
   *
   * @param mapExpr         A [[MapFields]] describing the map operation.
   * @param inputDataStream The Flink data stream being mapped.
   * @param lineageFactory  A factory for the lineage records that will be produced by the operation.
   * @return A [[SingleOutputStreamOperator]] of the mapped records and their associated lineage records.
   * @note This version of [[applyMapToFieldsFunction]] does not require the caller to know the input and output
   *       types of the map.
   */
  def applyMapToFieldsFunction(mapExpr: MapFields,
                               inputDataStream: SingleOutputStreamOperator[_],
                               metricFactory: MetricFactory,
                               lineageFactory: LineageRecordFactory): SingleOutputStreamOperator[RecordWithLineage[_]] = {
    val inputTypeInformation = inputDataStream.getType
    val inputTypeName = TypeUtil.getTypeName(inputTypeInformation)

    val outputTypeInformation = TupleStreamTypeInformation.createFromFieldDefinitions(mapExpr.fields)

    this.logger.info(s"ApplyMapToFieldsFunction from $inputTypeInformation to $outputTypeInformation.")

    val eval = RuntimeEvaluator.instance

    eval.evalFunction[MapFields, SingleOutputStreamOperator[_], MetricFactory, LineageRecordFactory, TypeInformation[_], SingleOutputStreamOperator[RecordWithLineage[_]]](
      "mapExpr",
      eval.getClassName[MapFields],
      "inputDataStream",
      FlinkTypeNames.singleOutputStreamOperator(inputTypeName),
      "metricFactory",
      MetricFactory.typeName,
      "lineageFactory",
      eval.getClassName[LineageRecordFactory],
      "outputTypeInformation",
      s"com.amazon.milan.flink.components.TupleStreamTypeInformation",
      s"${this.typeName}.applyMapToFieldsFunction[$inputTypeName](mapExpr, inputDataStream, metricFactory, lineageFactory, outputTypeInformation)",
      mapExpr,
      inputDataStream,
      metricFactory,
      lineageFactory,
      outputTypeInformation)
  }

  def applyMapToFieldsFunction[TIn <: Record](mapExpr: MapFields,
                                              inputDataStream: SingleOutputStreamOperator[TIn],
                                              metricFactory: MetricFactory,
                                              lineageFactory: LineageRecordFactory,
                                              outputTypeInformation: TupleStreamTypeInformation): SingleOutputStreamOperator[RecordWithLineage[ArrayRecord]] = {
    val flinkMapFunction = new MapToFieldsMapFunction[TIn](
      mapExpr,
      mapExpr.getInputRecordType,
      outputTypeInformation,
      metricFactory)

    val lineageWrapper = new LineageMapFunctionWrapper[TIn, ArrayRecord, MapToFieldsMapFunction[TIn]](flinkMapFunction, lineageFactory)

    inputDataStream.map(lineageWrapper).name(this.generateName(mapExpr))
  }

  private def generateName(mapExpr: MapNodeExpression): String = {
    s"Map [${mapExpr.source.nodeName}] -> [${mapExpr.nodeName}]"
  }
}
