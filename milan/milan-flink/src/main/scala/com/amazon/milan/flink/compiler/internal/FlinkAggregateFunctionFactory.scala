package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.flink._
import com.amazon.milan.flink.api.MilanAggregateFunction
import com.amazon.milan.flink.compiler.FlinkCompilationException
import com.amazon.milan.flink.components._
import com.amazon.milan.program._
import com.amazon.milan.typeutil.TypeDescriptor
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.datastream.{AllWindowedStream, SingleOutputStreamOperator, WindowedStream}
import org.apache.flink.streaming.api.functions.windowing.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.slf4j.LoggerFactory

import scala.language.existentials


object FlinkAggregateFunctionFactory {
  private val typeName: String = getClass.getTypeName.stripSuffix("$")
  private val logger = Logger(LoggerFactory.getLogger(getClass))

  /**
   * Applies a select() operation to a [[WindowedStream]] stream.
   *
   * @param mapExpr        The graph node corresponding to the select() operation.
   * @param windowedStream The Flink [[WindowedStream]] that will have the select operation applied.
   * @param windowTypeName The full name of the window type of the windowed stream.
   * @return The [[SingleOutputStreamOperator]] that is the result of applying the select() operation to the windowed stream.
   */
  def applySelectToWindowedStream(mapExpr: MapNodeExpression,
                                  windowedStream: WindowedStream[_, _, _],
                                  windowTypeName: String): SingleOutputStreamOperator[_] = {
    val eval = RuntimeEvaluator.instance

    val inputTypeName = windowedStream.getInputType.getTypeName
    val outputTypeInfo = eval.createTypeInformation(mapExpr.recordType)
    val outputTypeName = outputTypeInfo.getTypeName

    val groupExpr = mapExpr.source.asInstanceOf[GroupingExpression]
    val keyTypeName = groupExpr.expr.tpe.getTypeName

    eval.evalFunction[MapNodeExpression, GroupingExpression, WindowedStream[_, _, _], String, TypeInformation[_], SingleOutputStreamOperator[_]](
      "mapExpr",
      eval.getClassName[MapNodeExpression],
      "groupExpr",
      eval.getClassName[GroupingExpression],
      "windowedStream",
      FlinkTypeNames.windowedStream(inputTypeName, keyTypeName, FlinkTypeNames.window),
      "windowTypeName",
      "String",
      "outputTypeInfo",
      FlinkTypeNames.typeInformation(outputTypeName),
      s"${this.typeName}.applySelectToWindowedStreamImpl(mapExpr, groupExpr, windowedStream, windowTypeName, outputTypeInfo)",
      mapExpr,
      groupExpr,
      windowedStream,
      windowTypeName,
      outputTypeInfo)
  }

  /**
   * Applies a select() operation to a [[WindowedStream]] stream.
   *
   * @param mapExpr        The graph node corresponding to the select() operation.
   * @param groupExpr      A [[GroupingExpression]] representing the input to the aggregation.
   * @param windowedStream The Flink [[WindowedStream]] that will have the select operation applied.
   * @param windowTypeName The full name of the window type of the windowed stream.
   * @param outputTypeInfo A [[TypeInformation]] corresponding to the output type of the select() operation.
   * @tparam TIn     The record type of the windowed stream.
   * @tparam TKey    The key type of the windowed stream.
   * @tparam TWindow The window type of the windowed stream.
   * @tparam TOut    The output type of the select() operation.
   * @return The [[SingleOutputStreamOperator]] that is the result of applying the select() operation to the windowed stream.
   */
  def applySelectToWindowedStreamImpl[TIn, TKey, TWindow <: Window, TOut](mapExpr: MapNodeExpression,
                                                                          groupExpr: GroupingExpression,
                                                                          windowedStream: WindowedStream[TIn, TKey, TWindow],
                                                                          windowTypeName: String,
                                                                          outputTypeInfo: TypeInformation[TOut]): SingleOutputStreamOperator[TOut] = {
    val output =
      mapExpr match {
        case mapRecord: MapRecord =>
          this.applyMapToRecordAggregateFunction(mapRecord, groupExpr, windowedStream, windowTypeName, outputTypeInfo)

        case mapFields: MapFields =>
          this.applyMapToFieldsAggregateFunction(mapFields, groupExpr, windowedStream, windowTypeName, outputTypeInfo)
      }

    output.name(this.generateName(mapExpr))
  }

  /**
   * Applies a select() operation to a [[AllWindowedStream]] stream.
   *
   * @param mapExpr        The graph node corresponding to the select() operation.
   * @param windowedStream The Flink [[AllWindowedStream]] that will have the select operation applied.
   * @param windowTypeName The full name of the window type of the windowed stream.
   * @return The [[SingleOutputStreamOperator]] that is the result of applying the select() operation to the windowed stream.
   */
  def applySelectToAllWindowedStream(mapExpr: MapNodeExpression,
                                     windowedStream: AllWindowedStream[_, _],
                                     windowTypeName: String): SingleOutputStreamOperator[_] = {
    val eval = RuntimeEvaluator.instance
    val inputTypeName = windowedStream.getInputType.getTypeName
    val outputTypeInfo = eval.createTypeInformation(mapExpr.recordType)
    val outputTypeName = outputTypeInfo.getTypeName
    val groupExpr = mapExpr.source.asInstanceOf[GroupingExpression]

    eval.evalFunction[MapNodeExpression, GroupingExpression, AllWindowedStream[_, _], String, TypeInformation[_], SingleOutputStreamOperator[_]](
      "mapExpr",
      eval.getClassName[MapNodeExpression],
      "groupExpr",
      eval.getClassName[GroupingExpression],
      "windowedStream",
      FlinkTypeNames.allWindowedStream(inputTypeName, FlinkTypeNames.window),
      "windowTypeName",
      "String",
      "outputTypeInfo",
      FlinkTypeNames.typeInformation(outputTypeName),
      s"${this.typeName}.applySelectToAllWindowedStreamImpl(mapExpr, groupExpr, windowedStream, windowTypeName, outputTypeInfo)",
      mapExpr,
      groupExpr,
      windowedStream,
      windowTypeName,
      outputTypeInfo)
  }

  /**
   * Applies a select() operation to a [[AllWindowedStream]] stream.
   *
   * @param mapExpr        The graph node corresponding to the select() operation.
   * @param groupExpr      A [[GroupingExpression]] representing the input to the aggregation.
   * @param windowedStream The Flink [[AllWindowedStream]] that will have the select operation applied.
   * @param windowTypeName The full name of the window type of the windowed stream.
   * @param outputTypeInfo A [[TypeInformation]] corresponding to the output type of the select() operation.
   * @tparam TIn     The record type of the windowed stream.
   * @tparam TWindow The window type of the windowed stream.
   * @tparam TOut    The output type of the select() operation.
   * @return The [[SingleOutputStreamOperator]] that is the result of applying the select() operation to the windowed stream.
   */
  def applySelectToAllWindowedStreamImpl[TIn, TWindow <: Window, TOut](mapExpr: MapNodeExpression,
                                                                       groupExpr: GroupingExpression,
                                                                       windowedStream: AllWindowedStream[TIn, TWindow],
                                                                       windowTypeName: String,
                                                                       outputTypeInfo: TypeInformation[TOut]): SingleOutputStreamOperator[TOut] = {
    val output =
      mapExpr match {
        case mapRecord: MapRecord =>
          this.applyMapToRecordAggregateFunction(mapRecord, groupExpr, windowedStream, windowTypeName, outputTypeInfo)

        case mapFields: MapFields =>
          this.applyMapToFieldsAggregateFunction(mapFields, groupExpr, windowedStream, windowTypeName, outputTypeInfo)
      }

    output.name(this.generateName(mapExpr))
  }

  /**
   * Applies an aggregate select() operation that results in a single record to a windowed stream.
   */
  def applyMapToRecordAggregateFunction[TIn, TKey, TWindow <: Window, TOut](mapExpr: MapRecord,
                                                                            groupExpr: GroupingExpression,
                                                                            windowedStream: WindowedStream[TIn, TKey, TWindow],
                                                                            windowTypeName: String,
                                                                            outputTypeInfo: TypeInformation[TOut]): SingleOutputStreamOperator[TOut] = {
    val mapFunctionDef = mapExpr.expr
    val inputTypeName = windowedStream.getInputType.getTypeName

    val aggregateFunction = this.createIntermediateAggregateFunction[TIn](
      mapFunctionDef,
      inputTypeName,
      groupExpr)

    val aggregateOutputTypeInfo = aggregateFunction.getProducedType
    val aggregateOutputTypeName = aggregateOutputTypeInfo.getTypeName

    val accumulatorTypeInfo = aggregateFunction.getAccumulatorType
    val accumulatorTypeName = accumulatorTypeInfo.getTypeName
    val outputTypeName = outputTypeInfo.getTypeName
    val keyTypeName = groupExpr.expr.tpe.getTypeName

    val eval = RuntimeEvaluator.instance

    val processWindowFunction =
      eval.evalFunction[GroupingExpression, String, String, FunctionDef, ProcessWindowFunction[_, TOut, TKey, TWindow]](
        "groupExpr",
        eval.getClassName[GroupingExpression],
        "keyTypeName",
        "String",
        "inputTypeName",
        "String",
        "mapFunctionDef",
        eval.getClassName[FunctionDef],
        s"${this.typeName}.getProcessWindowFunction[$aggregateOutputTypeName, $keyTypeName, $outputTypeName](groupExpr, keyTypeName, inputTypeName, mapFunctionDef)",
        groupExpr,
        keyTypeName,
        aggregateOutputTypeName,
        mapFunctionDef)

    eval.evalFunction[WindowedStream[TIn, TKey, TWindow], AggregateFunction[TIn, _, _], ProcessWindowFunction[_, TOut, TKey, TWindow], TypeInformation[_], TypeInformation[_], TypeInformation[TOut], SingleOutputStreamOperator[TOut]](
      "inputWindowedStream",
      FlinkTypeNames.windowedStream(inputTypeName, keyTypeName, windowTypeName),
      "aggregateFunction",
      FlinkTypeNames.aggregateFunction(inputTypeName, accumulatorTypeName, aggregateOutputTypeName),
      "processWindowFunction",
      FlinkTypeNames.processWindowFunction(aggregateOutputTypeName, outputTypeName, keyTypeName, windowTypeName),
      "accumulatorTypeInfo",
      FlinkTypeNames.typeInformation(accumulatorTypeName),
      "aggregateOutputTypeInfo",
      FlinkTypeNames.typeInformation(aggregateOutputTypeName),
      "outputTypeInfo",
      FlinkTypeNames.typeInformation(outputTypeName),
      s"inputWindowedStream.aggregate[$accumulatorTypeName, $aggregateOutputTypeName, $outputTypeName](aggregateFunction, processWindowFunction, accumulatorTypeInfo, aggregateOutputTypeInfo, outputTypeInfo)",
      windowedStream,
      aggregateFunction,
      processWindowFunction,
      accumulatorTypeInfo,
      aggregateOutputTypeInfo,
      outputTypeInfo)
  }

  /**
   * Applies an aggregate select() operation that results in a single record to a windowed stream.
   */
  def applyMapToRecordAggregateFunction[TIn, TWindow <: Window, TOut](mapExpr: MapRecord,
                                                                      groupExpr: GroupingExpression,
                                                                      windowedStream: AllWindowedStream[TIn, TWindow],
                                                                      windowTypeName: String,
                                                                      outputTypeInfo: TypeInformation[TOut]): SingleOutputStreamOperator[TOut] = {
    val mapFunctionDef = mapExpr.expr
    val inputTypeName = windowedStream.getInputType.getTypeName

    val aggregateFunction = this.createIntermediateAggregateFunction[TIn](mapFunctionDef, inputTypeName, groupExpr)

    val aggregateOutputTypeInfo = aggregateFunction.getProducedType
    val aggregateOutputTypeName = aggregateOutputTypeInfo.getTypeName

    val accumulatorTypeInfo = aggregateFunction.getAccumulatorType
    val accumulatorTypeName = accumulatorTypeInfo.getTypeName
    val outputTypeName = outputTypeInfo.getTypeName

    val eval = RuntimeEvaluator.instance

    val processWindowFunction =
      eval.evalFunction[String, FunctionDef, ProcessAllWindowFunction[_, TOut, TWindow]](
        "inputTypeName",
        "String",
        "mapFunctionDef",
        eval.getClassName[FunctionDef],
        s"${this.typeName}.getProcessAllWindowFunction[$aggregateOutputTypeName, $outputTypeName](inputTypeName, mapFunctionDef)",
        aggregateOutputTypeName,
        mapFunctionDef)

    eval.evalFunction[AllWindowedStream[TIn, TWindow], AggregateFunction[TIn, _, _], ProcessAllWindowFunction[_, TOut, TWindow], TypeInformation[_], TypeInformation[_], TypeInformation[TOut], SingleOutputStreamOperator[TOut]](
      "inputWindowedStream",
      FlinkTypeNames.allWindowedStream(inputTypeName, windowTypeName),
      "aggregateFunction",
      FlinkTypeNames.aggregateFunction(inputTypeName, accumulatorTypeName, aggregateOutputTypeName),
      "processWindowFunction",
      FlinkTypeNames.processAllWindowFunction(aggregateOutputTypeName, outputTypeName, windowTypeName),
      "accumulatorTypeInfo",
      FlinkTypeNames.typeInformation(accumulatorTypeName),
      "aggregateOutputTypeInfo",
      FlinkTypeNames.typeInformation(aggregateOutputTypeName),
      "outputTypeInfo",
      FlinkTypeNames.typeInformation(outputTypeName),
      s"inputWindowedStream.aggregate(aggregateFunction, processWindowFunction, accumulatorTypeInfo, aggregateOutputTypeInfo, outputTypeInfo)",
      windowedStream,
      aggregateFunction,
      processWindowFunction,
      accumulatorTypeInfo,
      aggregateOutputTypeInfo,
      outputTypeInfo)
  }

  /**
   * Applies an aggregate select() operation that results in a list of fields to a windowed stream.
   */
  def applyMapToFieldsAggregateFunction[TIn, TKey, TWindow <: Window, TOut](mapExpr: MapFields,
                                                                            groupExpr: GroupingExpression,
                                                                            windowedStream: WindowedStream[TIn, TKey, TWindow],
                                                                            windowTypeName: String,
                                                                            outputTypeInfo: TypeInformation[TOut]): SingleOutputStreamOperator[TOut] = {
    val inputTypeName = windowedStream.getInputType.getTypeName

    val fieldAggregateFunctions = mapExpr.fields.map(f =>
      this.createIntermediateAggregateFunction[TIn](f.expr, inputTypeName, groupExpr))

    val fieldAggregateOutputTypeNames = fieldAggregateFunctions.map(_.getProducedType.getTypeName)

    val aggregateFunction = MultiAggregateFunction.combineAggregateFunctions[TIn](fieldAggregateFunctions, inputTypeName)
    val accumulatorTypeName = aggregateFunction.getAccumulatorType.getTypeName
    val outputTypeName = outputTypeInfo.getTypeName
    val keyTypeName = groupExpr.expr.tpe.getTypeName

    val aggregateOutputTypeInfo = aggregateFunction.getProducedType
    val aggregateOutputTypeName = aggregateOutputTypeInfo.getTypeName

    val eval = RuntimeEvaluator.instance

    val fieldFunctions = mapExpr.fields.map(_.expr)

    val processWindowFunction =
      eval.evalFunction[GroupingExpression, String, List[String], List[FunctionDef], ProcessWindowFunction[_, TOut, TKey, TWindow]](
        "groupExpr",
        eval.getClassName[GroupingExpression],
        "keyTypeName",
        "String",
        "fieldAggregateOutputTypeNames",
        "List[String]",
        "fieldFunctionDefs",
        s"List[${eval.getClassName[FunctionDef]}]",
        s"${this.typeName}.getFieldsProcessWindowFunction[$aggregateOutputTypeName, $outputTypeName, $keyTypeName, $windowTypeName](groupExpr, keyTypeName, fieldAggregateOutputTypeNames, fieldFunctionDefs)",
        groupExpr,
        keyTypeName,
        fieldAggregateOutputTypeNames,
        fieldFunctions)

    eval.evalFunction[WindowedStream[TIn, TKey, TWindow], MilanAggregateFunction[TIn, _, _], ProcessWindowFunction[_, TOut, TKey, TWindow], TypeInformation[_], TypeInformation[_], TypeInformation[TOut], SingleOutputStreamOperator[TOut]](
      "windowedStream",
      FlinkTypeNames.windowedStream(inputTypeName, keyTypeName, windowTypeName),
      "aggregateFunction",
      s"${MilanAggregateFunction.typeName}[$inputTypeName, $accumulatorTypeName, $aggregateOutputTypeName]",
      "processWindowFunction",
      FlinkTypeNames.processWindowFunction(aggregateOutputTypeName, outputTypeName, keyTypeName, windowTypeName),
      "accumulatorTypeInfo",
      FlinkTypeNames.typeInformation(accumulatorTypeName),
      "aggregateOutputTypeInfo",
      FlinkTypeNames.typeInformation(aggregateOutputTypeName),
      "outputTypeInfo",
      FlinkTypeNames.typeInformation(outputTypeName),
      "windowedStream.aggregate(aggregateFunction, processWindowFunction, accumulatorTypeInfo, aggregateOutputTypeInfo, outputTypeInfo)",
      windowedStream,
      aggregateFunction,
      processWindowFunction,
      aggregateFunction.getAccumulatorType,
      aggregateOutputTypeInfo,
      outputTypeInfo)
  }

  /**
   * Applies an aggregate select() operation that results in a list of fields to a windowed stream.
   */
  def applyMapToFieldsAggregateFunction[TIn, TWindow <: Window, TOut](mapExpr: MapFields,
                                                                      groupExpr: GroupingExpression,
                                                                      windowedStream: AllWindowedStream[TIn, TWindow],
                                                                      windowTypeName: String,
                                                                      outputTypeInfo: TypeInformation[TOut]): SingleOutputStreamOperator[TOut] = {
    val inputTypeName = windowedStream.getInputType.getTypeName

    val fieldAggregateFunctions = mapExpr.fields.map(f =>
      this.createIntermediateAggregateFunction[TIn](f.expr, inputTypeName, groupExpr))

    val fieldAggregateTypeNames = fieldAggregateFunctions.map(_.getProducedType.getTypeName)

    val aggregateFunction = MultiAggregateFunction.combineAggregateFunctions[TIn](fieldAggregateFunctions, inputTypeName)
    val accumulatorTypeName = aggregateFunction.getAccumulatorType.getTypeName
    val outputTypeName = outputTypeInfo.getTypeName

    val aggregateOutputTypeInfo = aggregateFunction.getProducedType
    val aggregateOutputTypeName = aggregateOutputTypeInfo.getTypeName

    val eval = RuntimeEvaluator.instance

    val fieldFunctions = mapExpr.fields.map(_.expr)

    val processWindowFunction =
      eval.evalFunction[List[String], List[FunctionDef], ProcessAllWindowFunction[_, TOut, TWindow]](
        "fieldAggregateTypeNames",
        "List[String]",
        "fieldFunctionDefs",
        s"List[${eval.getClassName[FunctionDef]}]",
        s"${this.typeName}.getFieldsProcessAllWindowFunction[$aggregateOutputTypeName, $outputTypeName](fieldAggregateTypeNames, fieldFunctionDefs)",
        fieldAggregateTypeNames,
        fieldFunctions)

    eval.evalFunction[AllWindowedStream[TIn, TWindow], MilanAggregateFunction[TIn, _, _], ProcessAllWindowFunction[_, TOut, TWindow], TypeInformation[_], TypeInformation[_], TypeInformation[TOut], SingleOutputStreamOperator[TOut]](
      "windowedStream",
      FlinkTypeNames.allWindowedStream(inputTypeName, windowTypeName),
      "aggregateFunction",
      s"${MilanAggregateFunction.typeName}[$inputTypeName, $accumulatorTypeName, $aggregateOutputTypeName]",
      "processWindowFunction",
      FlinkTypeNames.processAllWindowFunction(aggregateOutputTypeName, outputTypeName, windowTypeName),
      "accumulatorTypeInfo",
      FlinkTypeNames.typeInformation(accumulatorTypeName),
      "aggregateOutputTypeInfo",
      FlinkTypeNames.typeInformation(aggregateOutputTypeName),
      "outputTypeInfo",
      FlinkTypeNames.typeInformation(outputTypeName),
      s"windowedStream.aggregate[$accumulatorTypeName, $aggregateOutputTypeName, $outputTypeName](aggregateFunction, processWindowFunction, accumulatorTypeInfo, aggregateOutputTypeInfo, outputTypeInfo)",
      windowedStream,
      aggregateFunction,
      processWindowFunction,
      aggregateFunction.getAccumulatorType,
      aggregateOutputTypeInfo,
      outputTypeInfo)
  }

  /**
   * Creates a Flink [[AggregateFunction]] from a [[FunctionDef]] describing the aggregation function being applied.
   * The output of this aggregate function is an intermediate value that must be combined with the Key or Window in a
   * ProcessWindowFunction in order to produce the user's desired output type.
   *
   * @param mapFunctionDef A [[FunctionDef]] describing an aggregation function.
   * @param inputTypeName  The full name of the record type of the input windowed stream.
   * @param groupExpr      A [[GroupingExpression]] representing the input to the aggregation.
   * @tparam TIn The input record type to the map function.
   * @return A [[MilanAggregateFunction]] that will perform the aggregation.
   */
  def createIntermediateAggregateFunction[TIn](mapFunctionDef: FunctionDef,
                                               inputTypeName: String,
                                               groupExpr: GroupingExpression): MilanAggregateFunction[TIn, _, _] = {
    // The map function will be made up of zero or more function calls to aggregate functions, combined together
    // in some expression tree.
    // We need to separate out those aggregate function calls and create a flink AggregateFunction for each one.
    // Each of these separate AggregateFunctions will have the same input type, which is the input stream record type.
    val aggregateFunctionReferences = AggregateFunctionTreeExtractor.getAggregateExpressions(mapFunctionDef)
    val aggregateFunctionInputMaps = AggregateFunctionTreeExtractor.getAggregateInputFunctions(mapFunctionDef)
    val aggregateFunctions = aggregateFunctionReferences.zip(aggregateFunctionInputMaps)
      .map { case (expr, fun) => this.getAggregateFunction[TIn](expr, groupExpr, inputTypeName, fun) }

    // Combine all of the individual AggregateFunctions into a single AggregateFunction.
    MultiAggregateFunction.combineAggregateFunctions(aggregateFunctions, inputTypeName)
  }

  /**
   * Gets a [[MilanAggregateFunction]] instance corresponding the specified [[FunctionReference]] object.
   *
   * @param aggregateExpression An [[AggregateExpression]] representing the aggregate operation.
   * @param groupExpr           A [[GroupingExpression]] representing the input to the aggregation.
   * @param inputStreamTypeName The name of the record type of the input stream corresponding to the input node.
   * @param inputMapFunctionDef A [[FunctionDef]] describing a function that transforms input stream records into the
   *                            input for the aggregate function.
   * @tparam TIn The type of the input stream records.
   * @return A [[MilanAggregateFunction]] that takes input stream records as input and performs the aggregation
   *         referenced by the [[FunctionReference]].
   */
  private def getAggregateFunction[TIn](aggregateExpression: AggregateExpression,
                                        groupExpr: GroupingExpression,
                                        inputStreamTypeName: String,
                                        inputMapFunctionDef: FunctionDef): MilanAggregateFunction[TIn, _, _] = {
    val eval = RuntimeEvaluator.instance

    // Get a list of TypeInformation for the inputs to the input mapping function.
    // If the input type is a tuple then the mapping function takes more than one input.
    val inputTypeInfo = if (inputMapFunctionDef.tpe.isTuple) {
      inputMapFunctionDef.tpe.genericArguments.map(ty => eval.createTypeInformation(ty))
    }
    else {
      List(eval.createTypeInformation(inputMapFunctionDef.tpe))
    }

    val aggregateFunction = this.getFlinkAggregateFunction(aggregateExpression, inputTypeInfo)
    this.wrapAggregateFunctionWithKeyAndInputMap[TIn](groupExpr, inputStreamTypeName, inputMapFunctionDef, aggregateFunction)
  }

  /**
   * Gets the Flink aggregate function corresponding to a [[FunctionReference]].
   *
   * @param aggregateExpression  An [[AggregateExpression]] representing the aggregate operation.
   * @param inputTypeInformation A list of [[TypeInformation]] containing type information for the inputs to the
   *                             aggregate function.
   * @return The [[MilanAggregateFunction]] object that implements the aggregation used in the function reference.
   */
  private def getFlinkAggregateFunction(aggregateExpression: AggregateExpression,
                                        inputTypeInformation: List[TypeInformation[_]]): MilanAggregateFunction[_, _, _] = {
    val inputTypeNames = inputTypeInformation.map(_.getTypeName)
    val eval = RuntimeEvaluator.instance

    aggregateExpression match {
      case _: Sum =>
        eval.evalFunction[TypeInformation[_], MilanAggregateFunction[_, _, _]](
          "inputTypeInfo",
          FlinkTypeNames.typeInformation(inputTypeNames.head),
          s"new ${BuiltinAggregateFunctions.typeName}.Sum[${inputTypeNames.head}](inputTypeInfo)",
          inputTypeInformation.head)

      case _: Min =>
        eval.evalFunction[TypeInformation[_], MilanAggregateFunction[_, _, _]](
          "inputTypeInfo",
          FlinkTypeNames.typeInformation(inputTypeNames.head),
          s"new ${BuiltinAggregateFunctions.typeName}.Min[${inputTypeNames.head}](inputTypeInfo)",
          inputTypeInformation.head)

      case _: Max =>
        eval.evalFunction[TypeInformation[_], MilanAggregateFunction[_, _, _]](
          "inputTypeInfo",
          FlinkTypeNames.typeInformation(inputTypeNames.head),
          s"new ${BuiltinAggregateFunctions.typeName}.Max[${inputTypeNames.head}](inputTypeInfo)",
          inputTypeInformation.head)

      case _: Mean =>
        eval.evalFunction[TypeInformation[_], MilanAggregateFunction[_, _, _]](
          "inputTypeInfo",
          FlinkTypeNames.typeInformation(inputTypeNames.head),
          s"new ${BuiltinAggregateFunctions.typeName}.Mean[${inputTypeNames.head}](inputTypeInfo)",
          inputTypeInformation.head)

      case _: ArgMax =>
        eval.evalFunction[TypeInformation[_], TypeInformation[_], MilanAggregateFunction[_, _, _]](
          "input1TypeInfo",
          FlinkTypeNames.typeInformation(inputTypeNames.head),
          "input2TypeInfo",
          FlinkTypeNames.typeInformation(inputTypeNames.last),
          s"new ${BuiltinAggregateFunctions.typeName}.ArgMax[${inputTypeNames.head}, ${inputTypeNames.last}](input1TypeInfo, input2TypeInfo)",
          inputTypeInformation.head,
          inputTypeInformation.last)

      case _: ArgMin =>
        eval.evalFunction[TypeInformation[_], TypeInformation[_], MilanAggregateFunction[_, _, _]](
          "input1TypeInfo",
          FlinkTypeNames.typeInformation(inputTypeNames.head),
          "input2TypeInfo",
          FlinkTypeNames.typeInformation(inputTypeNames.last),
          s"new ${BuiltinAggregateFunctions.typeName}.ArgMin[${inputTypeNames.head}, ${inputTypeNames.last}](input1TypeInfo, input2TypeInfo)",
          inputTypeInformation.head,
          inputTypeInformation.last)

      case _: First =>
        eval.evalFunction[TypeInformation[_], MilanAggregateFunction[_, _, _]](
          "inputTypeInfo",
          FlinkTypeNames.typeInformation(inputTypeNames.head),
          s"new ${BuiltinAggregateFunctions.typeName}.Any[${inputTypeNames.head}](inputTypeInfo)",
          inputTypeInformation.head)

      case _ =>
        throw new FlinkCompilationException(s"Unsupported aggregation operation {$aggregateExpression.name}.")
    }
  }

  /**
   * Converts a [[MilanAggregateFunction]] into one that takes a different input type, by transforming the input
   * with a mapping function.
   *
   * @param groupExpr           A [[GroupingExpression]] representing the input to the aggregation.
   * @param inputStreamTypeName The full name of the record type of the input stream that is having the aggregate
   *                            function applied.
   * @param mapFunctionDef      The definition of the input mapping function.
   * @param aggregateFunction   The aggregate function to wrap.
   * @tparam TIn The type of the records of the input stream.
   * @return A [[MilanAggregateFunction]] that transforms input records and passes them to the supplied aggregate
   *         function.
   */
  private def wrapAggregateFunctionWithKeyAndInputMap[TIn](groupExpr: GroupingExpression,
                                                           inputStreamTypeName: String,
                                                           mapFunctionDef: FunctionDef,
                                                           aggregateFunction: MilanAggregateFunction[_, _, _]): MilanAggregateFunction[TIn, _, _] = {
    val inputType = groupExpr.getInputRecordType
    val accumulatorTypeName = aggregateFunction.getAccumulatorType.getTypeName
    val mappedTypeName = mapFunctionDef.tpe.getTypeName
    val outputTypeName = aggregateFunction.getProducedType.getTypeName
    val wrapperTypeName = s"${InputMappingAggregateFunctionWrapper.typeName}[$inputStreamTypeName, $mappedTypeName, $accumulatorTypeName, $outputTypeName]"

    val eval = RuntimeEvaluator.instance

    eval.evalFunction[TypeDescriptor[_], FunctionDef, MilanAggregateFunction[_, _, _], MilanAggregateFunction[TIn, _, _]](
      "inputType",
      TypeDescriptor.typeName("_"),
      "mapFunctionDef",
      eval.getClassName[FunctionDef],
      "aggregateFunction",
      s"${MilanAggregateFunction.typeName}[$mappedTypeName, $accumulatorTypeName, $outputTypeName]",
      s"new $wrapperTypeName(inputType, mapFunctionDef, aggregateFunction)",
      inputType,
      mapFunctionDef,
      aggregateFunction)
  }

  def getProcessWindowFunction[TAggOut, TKey, TOut](groupExpr: GroupingExpression,
                                                    keyTypeName: String,
                                                    inputTypeName: String,
                                                    mapFunctionDef: FunctionDef): ProcessWindowFunction[TAggOut, TOut, TKey, _] = {
    // Get the Milan FunctionDef for the function that converts the output tuple from the combined aggregation
    // function to the final output record type.
    val outputMapFunctionDef = AggregateFunctionTreeExtractor.getResultTupleToOutputFunction(mapFunctionDef)

    val inputTypeDesc = TypeDescriptor.forTypeName[Any](inputTypeName)

    groupExpr match {
      case TimeWindowExpression(GroupingExpression(_, _), _, _, _) =>
        new TimeWindowGroupByProcessWindowFunction[TAggOut, TKey, TOut](inputTypeDesc, outputMapFunctionDef)

      case _ =>
        new GroupByProcessWindowFunction[TAggOut, TKey, TOut](keyTypeName, inputTypeDesc, outputMapFunctionDef)
    }
  }

  def getProcessAllWindowFunction[TAggOut, TOut](inputTypeName: String,
                                                 mapFunctionDef: FunctionDef): ProcessAllWindowFunction[TAggOut, TOut, TimeWindow] = {
    val outputMapFunctionDef = AggregateFunctionTreeExtractor.getResultTupleToOutputFunction(mapFunctionDef)
    val inputTypeDesc = TypeDescriptor.forTypeName[Any](inputTypeName)
    new TimeWindowProcessAllWindowFunction[TAggOut, TOut](inputTypeDesc, outputMapFunctionDef)
  }

  def getFieldsProcessWindowFunction[TAggOut <: Tuple, TOut, TKey, TWindow <: Window](groupExpr: GroupingExpression,
                                                                                      keyTypeName: String,
                                                                                      fieldAggregateTypeNames: List[String],
                                                                                      fieldFunctionDefs: List[FunctionDef]): ProcessWindowFunction[TAggOut, TOut, TKey, TWindow] = {
    // We have a list of field functions and a list of the types returned by the intermediate aggregator for each field.

    // The field functions take two arguments, the first is the group key and the second is an input record.
    // We need to convert each field function into one that takes the result of the intermediate aggregator rather than
    // an input record.
    val fieldAggregateOutputMapFunctionDefs = fieldFunctionDefs.map(AggregateFunctionTreeExtractor.getResultTupleToOutputFunction)

    groupExpr match {
      case _: TimeWindowExpression =>
        new TimeWindowGroupByToFieldsProcessWindowFunction[TAggOut, TKey](fieldAggregateTypeNames, fieldAggregateOutputMapFunctionDefs)
          .asInstanceOf[ProcessWindowFunction[TAggOut, TOut, TKey, TWindow]]

      case _ =>
        new GroupByToFieldsProcessWindowFunction[TAggOut, TKey](keyTypeName, fieldAggregateTypeNames, fieldAggregateOutputMapFunctionDefs)
          .asInstanceOf[ProcessWindowFunction[TAggOut, TOut, TKey, TWindow]]
    }
  }

  def getFieldsProcessAllWindowFunction[TAggOut <: Tuple, TOut](fieldAggregateTypeNames: List[String],
                                                                fieldFunctionDefs: List[FunctionDef]): ProcessAllWindowFunction[TAggOut, TOut, TimeWindow] = {
    // We have a list of field functions and a list of the types returned by the intermediate aggregator for each field.

    // The field functions take two arguments, the first is the group key and the second is an input record.
    // We need to convert each field function into one that takes the result of the intermediate aggregator rather than
    // an input record.
    val fieldAggregateOutputMapFunctionDefs = fieldFunctionDefs.map(AggregateFunctionTreeExtractor.getResultTupleToOutputFunction)

    new TimeWindowToFieldsProcessWindowFunction[TAggOut](fieldAggregateTypeNames, fieldAggregateOutputMapFunctionDefs)
      .asInstanceOf[ProcessAllWindowFunction[TAggOut, TOut, TimeWindow]]
  }

  private def generateName(expr: MapNodeExpression): String = {
    s"AggregateSelect [${expr.nodeName}]"
  }
}
