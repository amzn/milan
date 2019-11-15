package com.amazon.milan.flink.compiler.internal

import java.time.Instant

import com.amazon.milan.flink._
import com.amazon.milan.flink.api.MilanAggregateFunction
import com.amazon.milan.flink.components._
import com.amazon.milan.program.{FunctionDef, GroupingExpression, MapFields, MapNodeExpression, MapRecord, SelectTerm, TimeWindowExpression, Tuple, UniqueBy}
import com.amazon.milan.typeutil.TypeDescriptor
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple => FlinkTuple}
import org.apache.flink.streaming.api.datastream.{AllWindowedStream, SingleOutputStreamOperator, WindowedStream}
import org.apache.flink.streaming.api.functions.windowing.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.windows.Window
import org.slf4j.LoggerFactory


object FlinkAggregateUniqueFunctionFactory {
  private val typeName: String = getClass.getTypeName.stripSuffix("$")
  private val logger = Logger(LoggerFactory.getLogger(getClass))

  trait Context[TIn, TWindow <: Window, TOut] {
    val inputType: TypeDescriptor[TIn]

    /**
     * The full name of the type of this [[Context]] object.
     */
    val typeName: String
  }

  class WindowedStreamContext[TIn, TKey, TWindow <: Window, TOut](val mapExpr: MapNodeExpression,
                                                                  val windowedStream: WindowedStream[TIn, TKey, TWindow],
                                                                  val windowTypeName: String,
                                                                  val outputTypeInfo: TypeInformation[TOut])
    extends Context[TIn, TWindow, TOut] {

    val uniqueExpr: UniqueBy = this.mapExpr.source.asInstanceOf[UniqueBy]

    val groupExpr: GroupingExpression = this.uniqueExpr.source.asInstanceOf[GroupingExpression]

    val inputType: TypeDescriptor[TIn] = this.groupExpr.getInputRecordType.asInstanceOf[TypeDescriptor[TIn]]

    val inputTypeInfo: TypeInformation[TIn] = this.windowedStream.getInputType

    val windowKeyFunctionDef: FunctionDef = this.groupExpr.expr

    val uniqueKeyFunctionDef: FunctionDef = this.uniqueExpr.expr

    // The default collision resolver for a uniqueness constraint is to take the newest value.
    val collisionResolverFunctionDef: FunctionDef = FunctionDef(List("currentValue", "newValue"), SelectTerm("newValue"))

    val inputTypeName: String = this.inputType.fullName

    val outputTypeName: String = this.outputTypeInfo.getTypeName

    val typeName = s"${FlinkAggregateUniqueFunctionFactory.typeName}.WindowedStreamContext[$inputTypeName, $windowKeyTypeName, $windowTypeName, $outputTypeName]"

    def windowKeyType: TypeDescriptor[TKey] = this.groupExpr.expr.tpe.asInstanceOf[TypeDescriptor[TKey]]

    def windowKeyTypeName: String = this.windowKeyType.fullName

    def mapRecord: MapRecord = this.mapExpr.asInstanceOf[MapRecord]

    def mapFields: MapFields = this.mapExpr.asInstanceOf[MapFields]
  }

  class AllWindowedStreamContext[TIn, TWindow <: Window, TOut](val mapExpr: MapNodeExpression,
                                                               val windowedStream: AllWindowedStream[TIn, TWindow],
                                                               val windowTypeName: String,
                                                               val outputTypeInfo: TypeInformation[TOut])
    extends Context[TIn, TWindow, TOut] {

    val uniqueExpr: UniqueBy = this.mapExpr.source.asInstanceOf[UniqueBy]

    val groupExpr: GroupingExpression = this.uniqueExpr.source.asInstanceOf[GroupingExpression]

    val inputType: TypeDescriptor[TIn] = this.groupExpr.getInputRecordType.asInstanceOf[TypeDescriptor[TIn]]

    val inputTypeInfo: TypeInformation[TIn] = this.windowedStream.getInputType

    val windowKeyFunctionDef: FunctionDef = this.groupExpr.expr

    val uniqueKeyFunctionDef: FunctionDef = this.uniqueExpr.expr

    // The default collision resolver for a uniqueness constraint is to take the newest value.
    val collisionResolverFunctionDef: FunctionDef = FunctionDef(List("currentValue", "newValue"), SelectTerm("newValue"))

    val inputTypeName: String = this.inputType.fullName

    val outputTypeName: String = this.outputTypeInfo.getTypeName

    val typeName = s"${FlinkAggregateUniqueFunctionFactory.typeName}.AllWindowedStreamContext[$inputTypeName, $windowTypeName, $outputTypeName]"

    val windowKeyType: TypeDescriptor[Instant] = com.amazon.milan.typeutil.types.Instant

    def mapRecord: MapRecord = this.mapExpr.asInstanceOf[MapRecord]

    def mapFields: MapFields = this.mapExpr.asInstanceOf[MapFields]
  }

  /**
   * Applies a select() operation to a [[WindowedStream]] stream.
   *
   * @param mapExpr        The map expression that represents the final output of the operation.
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
    val outputTypeName = mapExpr.getRecordTypeName

    val MapNodeExpression(UniqueBy(source, _)) = mapExpr
    val keyTypeName = source.asInstanceOf[GroupingExpression].expr.tpe.getTypeName

    val context = eval.evalFunction[MapNodeExpression, WindowedStream[_, _, _], String, TypeInformation[_], WindowedStreamContext[_, _, _, _]](
      "mapExpr",
      eval.getClassName[MapNodeExpression],
      "windowedStream",
      FlinkTypeNames.windowedStream(inputTypeName, keyTypeName, windowTypeName),
      "windowTypeName",
      "String",
      "outputTypeInfo",
      FlinkTypeNames.typeInformation(outputTypeName),
      s"new ${this.typeName}.WindowedStreamContext[$inputTypeName, $keyTypeName, $windowTypeName, $outputTypeName](mapExpr, windowedStream, windowTypeName, outputTypeInfo)",
      mapExpr,
      windowedStream,
      windowTypeName,
      outputTypeInfo)

    eval.evalFunction[WindowedStreamContext[_, _, _, _], SingleOutputStreamOperator[_]](
      "context",
      context.typeName,
      s"${this.typeName}.applySelectToWindowedStreamImpl(context)",
      context)
  }

  /**
   * Applies a select() operation to a [[WindowedStream]] stream.
   *
   * @param context The context for the call.
   * @tparam TIn     The record type of the windowed stream.
   * @tparam TKey    The key type of the windowed stream.
   * @tparam TWindow The window type of the windowed stream.
   * @tparam TOut    The output type of the select() operation.
   * @return The [[SingleOutputStreamOperator]] that is the result of applying the select() operation to the windowed stream.
   */
  def applySelectToWindowedStreamImpl[TIn, TKey, TWindow <: Window, TOut](context: WindowedStreamContext[TIn, TKey, TWindow, TOut]): SingleOutputStreamOperator[TOut] = {
    // There are two "keys" in use here. One is the "group key" which is the Flink window identifier. The other is the
    // "uniqueness key" which is computed using the expression provided to the unique() operation.
    // The map function (provided to the select() operation) is a tree where some of the nodes are aggregate operations
    // of input records, and other nodes combine these functions, possibly with the group key.
    // A UniqueValuesAggregator keeps a collection of all of the "uniqueness keys" and their corresponding records.
    // The output of the UniqueValuesAggregator is a tuple of the results of applying the aggregate operations to the
    // collected records.
    // This tuple is then sent to a ProcessWindowFunction which combines the aggregate operation results using the
    // remainder of the map function.

    val output =
      context.mapExpr match {
        case _: MapRecord =>
          this.applyMapToRecordAggregateFunction(context)

        case _: MapFields =>
          this.applyMapToFieldsAggregateFunction[TIn, TWindow, WindowedStreamContext[TIn, TKey, TWindow, TOut], TOut](context)
      }

    output.name(this.generateName(context.mapExpr))
  }

  /**
   * Applies a select() operation to a [[WindowedStream]] stream.
   *
   * @param mapExpr        The map expression that represents the final output of the operation.
   * @param windowedStream The Flink [[WindowedStream]] that will have the select operation applied.
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

    val context = eval.evalFunction[MapNodeExpression, AllWindowedStream[_, _], String, TypeInformation[_], AllWindowedStreamContext[_, _, _]](
      "mapExpr",
      eval.getClassName[MapNodeExpression],
      "windowedStream",
      FlinkTypeNames.allWindowedStream(inputTypeName, windowTypeName),
      "windowTypeName",
      "String",
      "outputTypeInfo",
      FlinkTypeNames.typeInformation(outputTypeName),
      s"new ${this.typeName}.AllWindowedStreamContext[$inputTypeName, $windowTypeName, $outputTypeName](mapExpr, windowedStream, windowTypeName, outputTypeInfo)",
      mapExpr,
      windowedStream,
      windowTypeName,
      outputTypeInfo)

    eval.evalFunction[AllWindowedStreamContext[_, _, _], SingleOutputStreamOperator[_]](
      "context",
      context.typeName,
      s"${this.typeName}.applySelectToAllWindowedStreamImpl(context)",
      context)
  }

  /**
   * Applies a select() operation to an [[AllWindowedStream]] stream.
   *
   * @param context The context of the aggregation operation.
   * @tparam TIn     The record type of the windowed stream.
   * @tparam TWindow The window type of the windowed stream.
   * @tparam TOut    The output type of the select() operation.
   * @return The [[SingleOutputStreamOperator]] that is the result of applying the select() operation to the windowed stream.
   */
  def applySelectToAllWindowedStreamImpl[TIn, TUniqueKey, TAggOut, TWindow <: Window, TOut](context: AllWindowedStreamContext[TIn, TWindow, TOut]): SingleOutputStreamOperator[TOut] = {
    val output =
      context.mapExpr match {
        case _: MapRecord =>
          this.applyMapToRecordAggregateFunction(context)

        case _: MapFields =>
          this.applyMapToFieldsAggregateFunction[TIn, TWindow, AllWindowedStreamContext[TIn, TWindow, TOut], TOut](context)
      }

    output.name(this.generateName(context.mapExpr))
  }

  /**
   * Applies an aggregate select() operation that results in a single record to a windowed stream.
   */
  private def applyMapToRecordAggregateFunction[TIn, TKey, TWindow <: Window, TOut](context: WindowedStreamContext[TIn, TKey, TWindow, TOut]): SingleOutputStreamOperator[TOut] = {
    val uniquenessKeyTypeName = context.uniqueKeyFunctionDef.tpe.getTypeName

    // Gets the parts of the tree that represent aggregate operations.
    // The result of the aggregate function is a tuple of the types of these expressions.
    val aggregateExpressions = AggregateFunctionTreeExtractor.getAggregateExpressions(context.mapRecord.expr)
    val aggregateExpressionTypes = aggregateExpressions.map(_.tpe)

    val eval = RuntimeEvaluator.instance

    val aggregateResultTypeInfo = eval.createTupleTypeInformation[org.apache.flink.api.java.tuple.Tuple](aggregateExpressionTypes)
    val aggregateResultTypeName = aggregateResultTypeInfo.getTypeName

    val methodName = s"${this.typeName}.applyMapToRecordAggregateFunctionImpl[${context.inputTypeName}, ${context.windowKeyTypeName}, $uniquenessKeyTypeName, $aggregateResultTypeName, ${context.windowTypeName}, ${context.outputTypeName}]"

    eval.evalFunction[WindowedStreamContext[TIn, TKey, TWindow, TOut], SingleOutputStreamOperator[TOut]](
      "context",
      context.typeName,
      s"$methodName(context)",
      context)
  }

  def applyMapToRecordAggregateFunctionImpl[TIn, TKey, TUniqueKey, TAggOut <: FlinkTuple, TWindow <: Window, TOut](context: WindowedStreamContext[TIn, TKey, TWindow, TOut]): SingleOutputStreamOperator[TOut] = {
    // First we need to create an AggregateFunction that collections input records and uses an output function
    // that applies the aggregate operations that we extract from the map function.
    val (aggregateFunction, aggregateResultTypeDesc) = this.createIntermediateAggregateFunction[TIn, TUniqueKey, TAggOut](
      context.inputType,
      context.windowedStream.getInputType,
      context.mapRecord.expr,
      context.uniqueKeyFunctionDef,
      context.collisionResolverFunctionDef)

    val eval = RuntimeEvaluator.instance
    val aggregateResultTypeInfo = eval.createTupleTypeInformation[TAggOut](aggregateResultTypeDesc.genericArguments)

    // Next we need to create a ProcessWindowFunction that takes the output from the aggregate function, which is a
    // tuple of the results of applying the aggregate operations, and combines it with the window key to produce
    // the final output record.
    val processWindowFunction = this.getProcessWindowFunction(context, aggregateResultTypeDesc)

    context.windowedStream.aggregate[UniqueValuesAccumulator[TIn, TUniqueKey], TAggOut, TOut](
      aggregateFunction,
      processWindowFunction,
      aggregateFunction.getAccumulatorType,
      aggregateResultTypeInfo,
      context.outputTypeInfo)
  }

  /**
   * Gets a [[ProcessWindowFunction]] that takes the output of the intermediate aggregation and applies the remaining
   * portion of the select function that combines the intermediate aggregation results with the group key.
   */
  private def getProcessWindowFunction[TAggOut, TOut, TKey, TWindow <: Window](context: WindowedStreamContext[_, TKey, TWindow, TOut],
                                                                               aggregateResultTypeDesc: TypeDescriptor[TAggOut]): ProcessWindowFunction[TAggOut, TOut, TKey, TWindow] = {
    val processWindowFunctionDef = AggregateFunctionTreeExtractor.getResultTupleToOutputFunction(context.mapRecord.expr)

    context.uniqueExpr.source match {
      case TimeWindowExpression(GroupingExpression(_, _), _, _, _) =>
        new TimeWindowGroupByProcessWindowFunction[TAggOut, TKey, TOut](
          aggregateResultTypeDesc,
          processWindowFunctionDef).asInstanceOf[ProcessWindowFunction[TAggOut, TOut, TKey, TWindow]]

      case _ =>
        new GroupByProcessWindowFunction[TAggOut, TKey, TOut](
          context.windowKeyTypeName,
          aggregateResultTypeDesc,
          processWindowFunctionDef).asInstanceOf[ProcessWindowFunction[TAggOut, TOut, TKey, TWindow]]
    }
  }

  /**
   * Applies an aggregate select() operation that results in a single record to a windowed stream.
   */
  private def applyMapToRecordAggregateFunction[TIn, TWindow <: Window, TOut](context: AllWindowedStreamContext[TIn, TWindow, TOut]): SingleOutputStreamOperator[TOut] = {
    val uniquenessKeyTypeName = context.uniqueKeyFunctionDef.tpe.getTypeName

    // Gets the parts of the tree that represent aggregate operations.
    // The result of the aggregate function is a tuple of the types of these expressions.
    val aggregateExpressions = AggregateFunctionTreeExtractor.getAggregateExpressions(context.mapRecord.expr)
    val aggregateExpressionTypes = aggregateExpressions.map(_.tpe)

    val eval = RuntimeEvaluator.instance

    val aggregateResultTypeInfo = eval.createTupleTypeInformation[org.apache.flink.api.java.tuple.Tuple](aggregateExpressionTypes)
    val aggregateResultTypeName = aggregateResultTypeInfo.getTypeName

    val methodName = s"${this.typeName}.applyMapToRecordAggregateFunctionImpl[${context.inputTypeName}, $uniquenessKeyTypeName, $aggregateResultTypeName, ${context.windowTypeName}, ${context.outputTypeName}]"

    eval.evalFunction[AllWindowedStreamContext[TIn, TWindow, TOut], SingleOutputStreamOperator[TOut]](
      "context",
      context.typeName,
      s"$methodName(context)",
      context)
  }

  def applyMapToRecordAggregateFunctionImpl[TIn, TUniqueKey, TAggOut <: FlinkTuple, TWindow <: Window, TOut](context: AllWindowedStreamContext[TIn, TWindow, TOut]): SingleOutputStreamOperator[TOut] = {
    // First we need to create an AggregateFunction that collections input records and uses an output function
    // that applies the aggregate operations that we extract from the map function.
    val (aggregateFunction, aggregateResultTypeDesc) = this.createIntermediateAggregateFunction[TIn, TUniqueKey, TAggOut](
      context.inputType,
      context.inputTypeInfo,
      context.mapRecord.expr,
      context.uniqueKeyFunctionDef,
      context.collisionResolverFunctionDef)

    val eval = RuntimeEvaluator.instance

    val aggregateResultTypeInfo = eval.createTupleTypeInformation[TAggOut](aggregateResultTypeDesc.genericArguments)

    // Next we need to create a ProcessAllWindowFunction that takes the output from the aggregate function, which is a
    // tuple of the results of applying the aggregate operations, and combines it with the window key to produce
    // the final output record.
    val processWindowFunctionDef = AggregateFunctionTreeExtractor.getResultTupleToOutputFunction(context.mapRecord.expr)
    val processWindowFunction =
      new TimeWindowProcessAllWindowFunction[TAggOut, TOut](aggregateResultTypeDesc, processWindowFunctionDef)
        .asInstanceOf[ProcessAllWindowFunction[TAggOut, TOut, TWindow]]

    context.windowedStream.aggregate(
      aggregateFunction,
      processWindowFunction,
      aggregateFunction.getAccumulatorType,
      aggregateResultTypeInfo,
      context.outputTypeInfo)
  }

  private def createIntermediateAggregateFunctionWithUnknownOutputType[TIn, TUniqueKey](inputType: TypeDescriptor[TIn],
                                                                                        inputTypeInfo: TypeInformation[TIn],
                                                                                        mapFunctionDef: FunctionDef,
                                                                                        uniqueKeyFunctionDef: FunctionDef,
                                                                                        collisionResolverFunctionDef: FunctionDef): (UniqueValuesAggregator[TIn, TUniqueKey, _], TypeDescriptor[_]) = {
    val aggregateExpressions = AggregateFunctionTreeExtractor.getAggregateExpressions(mapFunctionDef)
    val aggregateExpressionTypes = aggregateExpressions.map(_.tpe)
    val aggregateOutputType = TypeDescriptor.createTuple(FlinkTypeNames.tuple, aggregateExpressionTypes)
    val aggregateOutputTypeName = aggregateOutputType.fullName

    val eval = RuntimeEvaluator.instance
    val inputTypeName = inputType.fullName
    val uniqueKeyTypeName = uniqueKeyFunctionDef.tpe.fullName

    eval.evalFunction[TypeDescriptor[TIn], TypeInformation[TIn], FunctionDef, FunctionDef, FunctionDef, (UniqueValuesAggregator[TIn, TUniqueKey, _], TypeDescriptor[_])](
      "inputType",
      TypeDescriptor.typeName(inputTypeName),
      "inputTypeInfo",
      FlinkTypeNames.typeInformation(inputTypeName),
      "mapFunctionDef",
      eval.getClassName[FunctionDef],
      "uniqueKeyFunctionDef",
      eval.getClassName[FunctionDef],
      "collisionResolverFunctionDef",
      eval.getClassName[FunctionDef],
      s"${this.typeName}.createIntermediateAggregateFunction[$inputTypeName, $uniqueKeyTypeName, $aggregateOutputTypeName](inputType, inputTypeInfo, mapFunctionDef, uniqueKeyFunctionDef, collisionResolverFunctionDef)",
      inputType,
      inputTypeInfo,
      mapFunctionDef,
      uniqueKeyFunctionDef,
      collisionResolverFunctionDef)
  }

  def createIntermediateAggregateFunction[TIn, TUniqueKey, TAggOut <: FlinkTuple](inputType: TypeDescriptor[TIn],
                                                                                  inputTypeInfo: TypeInformation[TIn],
                                                                                  mapFunctionDef: FunctionDef,
                                                                                  uniqueKeyFunctionDef: FunctionDef,
                                                                                  collisionResolverFunctionDef: FunctionDef): (UniqueValuesAggregator[TIn, TUniqueKey, TAggOut], TypeDescriptor[TAggOut]) = {
    val uniqueKeyFunc = new RuntimeCompiledFunction[TIn, TUniqueKey](inputType, uniqueKeyFunctionDef)
    val collisionResolverFunc = new RuntimeCompiledFunction2[TIn, TIn, TIn](inputType, inputType, collisionResolverFunctionDef)

    // Gets the parts of the tree that represent aggregate operations. These are guaranteed not to reference the group
    // key argument.
    val aggregateExpressions = AggregateFunctionTreeExtractor.getAggregateExpressions(mapFunctionDef)

    // The first argument to the map function is the group key, so the last (second) argument is the input record.
    val inputArgName = mapFunctionDef.arguments.last

    // Create a function from the input record to a tuple containing the results of all of the aggregate expressions.
    // Later on we'll pass this tuple into a function that also takes the group key and computes the final result.
    val intermediateOutputTupleFunctionDef = FunctionDef(List(inputArgName), Tuple(aggregateExpressions))
    val iterableInputType = TypeDescriptor.iterableOf[TIn](inputType)
    val intermediateOutputTupleFunc = new RuntimeCompiledFunction[Iterable[TIn], TAggOut](iterableInputType, intermediateOutputTupleFunctionDef)

    val eval = RuntimeEvaluator.instance

    val aggregateExpressionTypes = aggregateExpressions.map(_.tpe)
    val aggregateResultTypeDesc = TypeDescriptor.createTuple[TAggOut](FlinkTypeNames.tuple, aggregateExpressionTypes)
    val aggregateResultTypeInfo = eval.createTupleTypeInformation[TAggOut](aggregateExpressionTypes)

    val uniqueKeyTypeInfo = eval.createTypeInformation[TUniqueKey](uniqueKeyFunctionDef.tpe.asInstanceOf[TypeDescriptor[TUniqueKey]])

    val aggregateFunction =
      new UniqueValuesAggregator[TIn, TUniqueKey, TAggOut](
        uniqueKeyFunc,
        collisionResolverFunc,
        intermediateOutputTupleFunc,
        inputTypeInfo,
        uniqueKeyTypeInfo,
        aggregateResultTypeInfo)

    (aggregateFunction, aggregateResultTypeDesc)
  }

  /**
   * Applies an aggregate select() operation that results in a list of fields to a windowed stream.
   */
  private def applyMapToFieldsAggregateFunction[TIn, TWindow <: Window, TContext <: Context[TIn, TWindow, TOut], TOut](context: TContext): SingleOutputStreamOperator[TOut] = {
    val eval = RuntimeEvaluator.instance

    eval.evalFunction[TContext, SingleOutputStreamOperator[TOut]](
      "context",
      context.typeName,
      s"${this.typeName}.applyMapToFieldsAggregateFunctionImpl(context)",
      context)
  }

  def applyMapToFieldsAggregateFunctionImpl[TIn, TKey, TUniqueKey, TWindow <: Window, TOut](context: WindowedStreamContext[TIn, TKey, TWindow, TOut]): SingleOutputStreamOperator[TOut] = {
    val fieldAggregateFunctionsResult = context.mapFields.fields.map(f =>
      this.createIntermediateAggregateFunctionWithUnknownOutputType[TIn, TUniqueKey](
        context.inputType,
        context.inputTypeInfo,
        f.expr,
        context.uniqueKeyFunctionDef,
        context.collisionResolverFunctionDef))

    val fieldAggregateFunctions = fieldAggregateFunctionsResult.map(_._1)
    val fieldAggregateOutputTypes = fieldAggregateFunctionsResult.map(_._2)
    val fieldAggregateOutputTypeNames = fieldAggregateOutputTypes.map(_.fullName)

    val aggregateFunction = MultiAggregateFunction.combineAggregateFunctions[TIn](
      fieldAggregateFunctions,
      context.inputTypeName)

    val accumulatorTypeName = aggregateFunction.getAccumulatorType.getTypeName

    val aggregateOutputTypeInfo = aggregateFunction.getProducedType
    val aggregateOutputTypeName = aggregateOutputTypeInfo.getTypeName

    val eval = RuntimeEvaluator.instance

    val fieldFunctionDefinitions = context.mapFields.fields.map(_.expr)
    val methodName = s"${this.typeName}.getFieldsProcessWindowFunction[$aggregateOutputTypeName, ${context.outputTypeName}, ${context.windowKeyTypeName}, ${context.windowTypeName}]"

    val processWindowFunction =
      eval.evalFunction[WindowedStreamContext[TIn, TKey, TWindow, TOut], List[String], List[FunctionDef], ProcessWindowFunction[_, TOut, TKey, TWindow]](
        "context",
        context.typeName,
        "fieldAggregateOutputTypeNames",
        "List[String]",
        "fieldFunctionDefs",
        s"List[${eval.getClassName[FunctionDef]}]",
        s"$methodName(context, fieldAggregateOutputTypeNames, fieldFunctionDefs)",
        context,
        fieldAggregateOutputTypeNames,
        fieldFunctionDefinitions)

    eval.evalFunction[WindowedStream[TIn, TKey, TWindow], MilanAggregateFunction[TIn, _, _], ProcessWindowFunction[_, TOut, TKey, TWindow], TypeInformation[_], TypeInformation[_], TypeInformation[TOut], SingleOutputStreamOperator[TOut]](
      "windowedStream",
      FlinkTypeNames.windowedStream(context.inputTypeName, context.windowKeyTypeName, context.windowTypeName),
      "aggregateFunction",
      s"${MilanAggregateFunction.typeName}[${context.inputTypeName}, $accumulatorTypeName, $aggregateOutputTypeName]",
      "processWindowFunction",
      FlinkTypeNames.processWindowFunction(aggregateOutputTypeName, context.outputTypeName, context.windowKeyTypeName, context.windowTypeName),
      "accumulatorTypeInfo",
      FlinkTypeNames.typeInformation(accumulatorTypeName),
      "aggregateOutputTypeInfo",
      FlinkTypeNames.typeInformation(aggregateOutputTypeName),
      "outputTypeInfo",
      FlinkTypeNames.typeInformation(context.outputTypeName),
      "windowedStream.aggregate(aggregateFunction, processWindowFunction, accumulatorTypeInfo, aggregateOutputTypeInfo, outputTypeInfo)",
      context.windowedStream,
      aggregateFunction,
      processWindowFunction,
      aggregateFunction.getAccumulatorType,
      aggregateOutputTypeInfo,
      context.outputTypeInfo)
  }

  def applyMapToFieldsAggregateFunctionImpl[TIn, TUniqueKey, TWindow <: Window, TOut](context: AllWindowedStreamContext[TIn, TWindow, TOut]): SingleOutputStreamOperator[TOut] = {
    val fieldAggregateFunctionsResult = context.mapFields.fields.map(f =>
      this.createIntermediateAggregateFunctionWithUnknownOutputType[TIn, TUniqueKey](
        context.inputType,
        context.inputTypeInfo,
        f.expr,
        context.uniqueKeyFunctionDef,
        context.collisionResolverFunctionDef))

    val fieldAggregateFunctions = fieldAggregateFunctionsResult.map(_._1)
    val fieldAggregateOutputTypes = fieldAggregateFunctionsResult.map(_._2)
    val fieldAggregateOutputTypeNames = fieldAggregateOutputTypes.map(_.fullName)

    val aggregateFunction = MultiAggregateFunction.combineAggregateFunctions[TIn](
      fieldAggregateFunctions,
      context.inputTypeName)

    val accumulatorTypeName = aggregateFunction.getAccumulatorType.getTypeName

    val aggregateOutputTypeInfo = aggregateFunction.getProducedType
    val aggregateOutputTypeName = aggregateOutputTypeInfo.getTypeName

    val eval = RuntimeEvaluator.instance

    val fieldFunctionDefinitions = context.mapFields.fields.map(_.expr)
    val methodName = s"${this.typeName}.getFieldsProcessAllWindowFunction[$aggregateOutputTypeName, ${context.outputTypeName}, ${context.windowTypeName}]"

    val processWindowFunction =
      eval.evalFunction[List[String], List[FunctionDef], ProcessAllWindowFunction[_, TOut, TWindow]](
        "fieldAggregateOutputTypeNames",
        "List[String]",
        "fieldFunctionDefs",
        s"List[${eval.getClassName[FunctionDef]}]",
        s"$methodName(fieldAggregateOutputTypeNames, fieldFunctionDefs)",
        fieldAggregateOutputTypeNames,
        fieldFunctionDefinitions)

    eval.evalFunction[AllWindowedStream[TIn, TWindow], MilanAggregateFunction[TIn, _, _], ProcessAllWindowFunction[_, TOut, TWindow], TypeInformation[_], TypeInformation[_], TypeInformation[TOut], SingleOutputStreamOperator[TOut]](
      "windowedStream",
      FlinkTypeNames.allWindowedStream(context.inputTypeName, context.windowTypeName),
      "aggregateFunction",
      s"${MilanAggregateFunction.typeName}[${context.inputTypeName}, $accumulatorTypeName, $aggregateOutputTypeName]",
      "processWindowFunction",
      FlinkTypeNames.processAllWindowFunction(aggregateOutputTypeName, context.outputTypeName, context.windowTypeName),
      "accumulatorTypeInfo",
      FlinkTypeNames.typeInformation(accumulatorTypeName),
      "aggregateOutputTypeInfo",
      FlinkTypeNames.typeInformation(aggregateOutputTypeName),
      "outputTypeInfo",
      FlinkTypeNames.typeInformation(context.outputTypeName),
      "windowedStream.aggregate(aggregateFunction, processWindowFunction, accumulatorTypeInfo, aggregateOutputTypeInfo, outputTypeInfo)",
      context.windowedStream,
      aggregateFunction,
      processWindowFunction,
      aggregateFunction.getAccumulatorType,
      aggregateOutputTypeInfo,
      context.outputTypeInfo)
  }


  private def generateName(mapExpr: MapNodeExpression): String = {
    s"AggregateUniqueSelect [${mapExpr.nodeName}]"
  }

  def getFieldsProcessWindowFunction[TAggOut <: FlinkTuple, TOut, TKey, TWindow <: Window](context: WindowedStreamContext[_, TKey, TWindow, TOut],
                                                                                           fieldAggregateTypeNames: List[String],
                                                                                           fieldFunctionDefs: List[FunctionDef]): ProcessWindowFunction[TAggOut, TOut, TKey, TWindow] = {
    // We have a list of field functions and a list of the types returned by the intermediate aggregator for each field.

    // The field functions take two arguments, the first is the group key and the second is an input record.
    // We need to convert each field function into one that takes the result of the intermediate aggregator rather than
    // an input record.
    val fieldAggregateOutputMapFunctionDefs = fieldFunctionDefs.map(AggregateFunctionTreeExtractor.getResultTupleToOutputFunction)

    context.uniqueExpr.source match {
      case TimeWindowExpression(GroupingExpression(_, _), _, _, _) =>
        new TimeWindowGroupByToFieldsProcessWindowFunction[TAggOut, TKey](fieldAggregateTypeNames, fieldAggregateOutputMapFunctionDefs)
          .asInstanceOf[ProcessWindowFunction[TAggOut, TOut, TKey, TWindow]]

      case _ =>
        new GroupByToFieldsProcessWindowFunction[TAggOut, TKey](context.windowKeyTypeName, fieldAggregateTypeNames, fieldAggregateOutputMapFunctionDefs)
          .asInstanceOf[ProcessWindowFunction[TAggOut, TOut, TKey, TWindow]]
    }
  }

  def getFieldsProcessAllWindowFunction[TAggOut <: FlinkTuple, TOut, TWindow <: Window](fieldAggregateTypeNames: List[String],
                                                                                        fieldFunctionDefs: List[FunctionDef]): ProcessAllWindowFunction[TAggOut, TOut, TWindow] = {
    val fieldAggregateOutputMapFunctionDefs = fieldFunctionDefs.map(AggregateFunctionTreeExtractor.getResultTupleToOutputFunction)

    new TimeWindowToFieldsProcessWindowFunction[TAggOut](fieldAggregateTypeNames, fieldAggregateOutputMapFunctionDefs)
      .asInstanceOf[ProcessAllWindowFunction[TAggOut, TOut, TWindow]]
  }
}
