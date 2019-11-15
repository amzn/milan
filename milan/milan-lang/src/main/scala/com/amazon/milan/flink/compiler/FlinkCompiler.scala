package com.amazon.milan.flink.compiler

import java.io.InputStream

import com.amazon.milan.SemanticVersion
import com.amazon.milan.application.metrics.HistogramDefinition
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.aws.metrics.CompiledMetric
import com.amazon.milan.flink.application.{FlinkApplicationConfiguration, FlinkApplicationInstance, FlinkDataSink}
import com.amazon.milan.flink.compiler.internal.FlinkWindowedStreamFactory.{ApplyGroupByWindowResult, ApplyTimeWindowResult}
import com.amazon.milan.flink.compiler.internal._
import com.amazon.milan.flink.metrics.OperatorMetricFactory
import com.amazon.milan.flink.{FlinkTypeNames, RuntimeEvaluator}
import com.amazon.milan.lang.StreamGraph
import com.amazon.milan.program._
import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.types.{LineageRecord, RecordWithLineage}
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.datastream.{WindowedStream => _, _}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable
import scala.language.existentials


object FlinkCompiler {
  val defaultCompiler = new FlinkCompiler()
}


case class CompilationResult(compiledMetrics: List[CompiledMetric]) extends Serializable


class FlinkCompiler(classLoader: ClassLoader) {
  private val logger = Logger(LoggerFactory.getLogger(getClass))
  private val eval = new RuntimeEvaluator(classLoader)

  /**
   * Initializes an instance of the [[FlinkCompiler]] class using the default [[ClassLoader]].
   */
  def this() {
    this(getClass.getClassLoader)
  }

  /**
   * Compiles an application instance defined by a [[StreamGraph]] and an [[ApplicationConfiguration]]
   * into a streaming environment.
   *
   * @param graph             A [[StreamGraph]] defining the application.
   * @param config            An [[ApplicationConfiguration]] containing the application configuration.
   * @param targetEnvironment The Flink [[StreamExecutionEnvironment]] into which the application will be compiled.
   * @return A [[CompilationResult]] containing the results of a successful compilation.
   */
  def compile(graph: StreamGraph,
              config: ApplicationConfiguration,
              targetEnvironment: StreamExecutionEnvironment): CompilationResult = {
    val application = new Application(graph)
    val instance = new ApplicationInstance(application, config)
    val json = ScalaObjectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(instance)
    this.compileFromInstanceJson(json, targetEnvironment)
  }

  /**
   * Compiles an application instance defined by a [[StreamGraph]] and a [[FlinkApplicationConfiguration]]
   * into a streaming environment.
   *
   * @param graph             A [[StreamGraph]] defining the application.
   * @param config            A [[FlinkApplicationConfiguration]] containing the application configuration.
   * @param targetEnvironment The Flink [[StreamExecutionEnvironment]] into which the application will be compiled.
   * @return A [[CompilationResult]] containing the results of a successful compilation.
   */
  def compile(graph: StreamGraph,
              config: FlinkApplicationConfiguration,
              targetEnvironment: StreamExecutionEnvironment): CompilationResult = {
    val instance = new FlinkApplicationInstance(new Application("", graph), config)
    this.compile(instance, targetEnvironment)
  }

  /**
   * Compiles an application instance defined by a [[FlinkApplicationInstance]] into a streaming environment.
   *
   * @param applicationInstance A [[FlinkApplicationInstance]] containing the instance to compile.
   * @param targetEnvironment   The Flink [[StreamExecutionEnvironment]] into which the application will be compiled.
   * @return A [[CompilationResult]] containing the results of a successful compilation.
   */
  def compile(applicationInstance: FlinkApplicationInstance,
              targetEnvironment: StreamExecutionEnvironment): CompilationResult = {
    val finalGraph = applicationInstance.application.graph.getDereferencedGraph
    GraphTypeChecker.typeCheckGraph(finalGraph)

    val env = new CompilationEnvironment(
      applicationInstance.instanceDefinitionId,
      finalGraph,
      applicationInstance.config,
      targetEnvironment)

    RuntimeEvaluator.instance = new RuntimeEvaluator(this.classLoader)

    env.graph.getStreams.foreach(stream => this.ensureStreamIsCompiled(env, stream.getStreamExpression))

    env.config.dataSinks.foreach(sink => this.compileSink(env, sink.streamId, sink.sink))

    this.addLineageSinks(env)

    CompilationResult(env.compiledMetrics.toList)
  }

  /**
   * Compile an application instance from a json representation of the instance definition.
   *
   * @param applicationInstanceJson A string containing a json representation of an application instance.
   * @param targetEnvironment       The Flink streaming environment to use as the compilation target.
   */
  def compileFromInstanceJson(applicationInstanceJson: String,
                              targetEnvironment: StreamExecutionEnvironment): CompilationResult = {
    RuntimeEvaluator.instance = new RuntimeEvaluator(this.classLoader)

    val instance = ScalaObjectMapper.readValue[FlinkApplicationInstance](
      applicationInstanceJson,
      classOf[FlinkApplicationInstance])

    this.compile(instance, targetEnvironment)
  }

  /**
   * Compile an application instance from a json representation of the instance definition.
   *
   * @param inputStream       A stream containing a json representation of an application instance.
   * @param targetEnvironment The Flink streaming environment to use as the compilation target.
   */
  def compileFromInstanceJson(inputStream: InputStream,
                              targetEnvironment: StreamExecutionEnvironment): CompilationResult = {
    RuntimeEvaluator.instance = new RuntimeEvaluator(this.classLoader)

    val instance = ScalaObjectMapper.readValue[FlinkApplicationInstance](inputStream, classOf[FlinkApplicationInstance])
    this.compile(instance, targetEnvironment)
  }

  /**
   * Ensures that a stream is compiled into the Flink environment.
   *
   * @param env        The compilation environment.
   * @param streamExpr The stream to compile.
   */
  private def ensureStreamIsCompiled(env: CompilationEnvironment, streamExpr: StreamExpression): Unit = {
    this.getOrCompileDataStream(env, streamExpr)
  }

  /**
   * Adds all lineage sinks from the application configuration as sinks for the compiled lineage streams.
   *
   * @param env The compilation environment.
   */
  private def addLineageSinks(env: CompilationEnvironment): Unit = {
    env.config.lineageSinks
      .map(_.getSinkFunction.asInstanceOf[SinkFunction[LineageRecord]])
      .foreach(sink => this.addLineageSink(env, sink))
  }

  /**
   * Adds a [[SinkFunction]]`]`[[LineageRecord]]`]` as a sink for all lineage streams.
   *
   * @param env  The compilation environment.
   * @param sink The sink function to add.
   */
  private def addLineageSink(env: CompilationEnvironment, sink: SinkFunction[LineageRecord]): Unit = {
    env.lineageStreams.foreach(stream => stream.addSink(sink))
  }

  /**
   * Gets a compiled [[DataStream]] for a graph node, compiling it if necessary.
   *
   * @param env        The compilation environment.
   * @param streamExpr The stream whose compiled [[DataStream]] will be returned.
   * @return The compiled [[DataStream]] corresponding to the graph node.
   */
  private def getOrCompileDataStream(env: CompilationEnvironment, streamExpr: StreamExpression): SingleOutputStreamOperator[_] = {
    env.compiledDataStreams.getOrElseUpdate(streamExpr.nodeId, compileDataStream(env, streamExpr))
  }

  /**
   * Gets a compiled [[ConnectedStreams]] for an expression, compiling it if necessary.
   *
   * @param env      The compilation environment.
   * @param joinExpr The expression whose compiled [[ConnectedStreams]] will be returned.
   * @return A [[ConnectStreamsResult]] containing the compiled [[ConnectedStreams]] corresponding to the graph node.
   */
  private def getOrCompileConnectedStream(env: CompilationEnvironment, joinExpr: JoinNodeExpression): ConnectStreamsResult = {
    env.compiledConnectedStreams.getOrElseUpdate(joinExpr, compileConnectedStream(env, joinExpr))
  }

  /**
   * Gets a compiled [[ApplyWindowResult]] for an expression, compiling it if necessary.
   *
   * @param env       The compilation environment.
   * @param groupExpr The expression whose compiled [[ApplyWindowResult]] will be returned.
   * @return An [[ApplyGroupByWindowResult]] containing the compiled [[ApplyWindowResult]] corresponding to the graph node.
   */
  private def getOrCompileWindowedStream(env: CompilationEnvironment,
                                         groupExpr: GroupingExpression): ApplyWindowResult = {
    env.compiledWindowedStreams.getOrElseUpdate(groupExpr, compileWindowedStream(env, groupExpr))
  }

  /**
   * Compiles a sink into the streaming environment.
   *
   * @param env      The compilation environment.
   * @param streamId The ID of the stream being connected to the sink.
   * @param sink     A [[FlinkDataSink]] representing the sink to compile.
   */
  private def compileSink(env: CompilationEnvironment, streamId: String, sink: FlinkDataSink[_]): Unit = {
    this.logger.info(s"Compiling sink for stream with ID '$streamId'.")

    val dataStream = env.compiledDataStreams(streamId)
    val stream = env.graph.getStream(streamId)
    val recordTypeName = stream.getStreamExpression.getRecordTypeName

    val streamSink =
      this.eval.evalFunction[DataStream[_], SinkFunction[_], DataStreamSink[_]](
        "stream",
        FlinkTypeNames.dataStream(recordTypeName),
        "sinkFunction",
        FlinkTypeNames.sinkFunction(recordTypeName),
        "stream.addSink(sinkFunction)",
        dataStream,
        sink.getSinkFunction)

    streamSink.name(stream.name)
  }

  /**
   * Compiles a graph node into a [[DataStream]] in the streaming environment.
   *
   * @param env        The compilation environment.
   * @param streamExpr A expression representing the stream to compile.
   * @return The [[DataStream]] corresponding to the graph node.
   */
  private def compileDataStream(env: CompilationEnvironment, streamExpr: StreamExpression): SingleOutputStreamOperator[_] = {
    this.logger.info(s"Compiling Flink data stream for node '${streamExpr.nodeName}' with ID '${streamExpr.nodeId}'. Expression: '$streamExpr'.")

    val compiledStream =
      streamExpr match {
        case r: Ref =>
          this.compileExternalStream(env, r)

        case f: Filter =>
          this.compileFilteredStream(env, f)

        case m: MapNodeExpression =>
          compileMappedStream(env, m)
      }

    this.compileMetrics(env, compiledStream, streamExpr)

    compiledStream
  }

  /**
   * Compiles a [[JoinNodeExpression]] into a Flink [[ConnectedStreams]].
   *
   * @return A [[ConnectStreamsResult]] that contains the compiled [[ConnectedStreams]] object and a Milan function
   *         containing the portion of the join condition expression that must be applied in the Flink
   *         [[CoProcessFunction]].
   */
  private def compileConnectedStream(env: CompilationEnvironment, joinExpr: JoinNodeExpression): ConnectStreamsResult = {
    compileJoinedStreamWithCondition(env, joinExpr)
  }

  /**
   * Compiles a stream with an external source.
   *
   * @param env        The compilation environment.
   * @param streamExpr The expression to compile.
   * @return A [[SingleOutputStreamOperator]] representing the stream in the Flink application.
   */
  private def compileExternalStream(env: CompilationEnvironment, streamExpr: Ref): SingleOutputStreamOperator[_] = {
    this.logger.info(s"Compiling ExternalStream '${streamExpr.nodeName}'.")
    val source = env.config.getSource(streamExpr.nodeId)

    source.addDataSource(env.streamEnvironment).name(streamExpr.nodeName)
  }

  /**
   * Compiles a stream that is the result of applying a filter operation to another stream.
   *
   * @param env        The compilation environment.
   * @param filterExpr The Filter expression.
   * @return A [[SingleOutputStreamOperator]] representing the output filtered stream in the Flink application.
   */
  private def compileFilteredStream(env: CompilationEnvironment, filterExpr: Filter): SingleOutputStreamOperator[_] = {
    val inputStream = this.getOrCompileDataStream(env, filterExpr.source)
    val metricFactory = new OperatorMetricFactory(env.config.metricPrefix, filterExpr.nodeName)

    FlinkFilterFunctionFactory.applyFilter(filterExpr, inputStream, metricFactory)
  }

  /**
   * Compiles a [[MapNodeExpression]] into a Flink [[SingleOutputStreamOperator]].
   */
  private def compileMappedStream(env: CompilationEnvironment, expr: MapNodeExpression): SingleOutputStreamOperator[_] = {
    expr.source match {
      case _: JoinNodeExpression =>
        compileJoinSelect(env, expr)

      case _: UniqueBy =>
        compileUniqueGroupSelect(env, expr)

      case _: GroupingExpression =>
        compileGroupSelect(env, expr)

      case _ =>
        compileMappedDataStream(env, expr)
    }
  }

  /**
   * Compiles a mapped stream that is the result of a grouping operation followed by a select statement.
   */
  private def compileGroupSelect(env: CompilationEnvironment,
                                 mapExpr: MapNodeExpression): SingleOutputStreamOperator[_] = {
    val windowResult = this.getOrCompileWindowedStream(env, mapExpr.source.asInstanceOf[GroupingExpression])

    windowResult match {
      case GroupByWindowResult(result) =>
        FlinkAggregateFunctionFactory.applySelectToWindowedStream(
          mapExpr,
          result.windowedStream,
          result.windowTypeName)

      case UnkeyedTimeWindowResult(result) =>
        FlinkAggregateFunctionFactory.applySelectToAllWindowedStream(
          mapExpr,
          result.windowedStream,
          FlinkTypeNames.timeWindow)

      case KeyedTimeWindowResult(result) =>
        FlinkAggregateFunctionFactory.applySelectToWindowedStream(
          mapExpr,
          result.windowedStream,
          result.windowTypeName)
    }
  }

  /**
   * Gets a [[KeyedStream]] from a data stream and a [[GroupBy]] expression containing the key function.
   */
  private def compileKeyedStreamForTimeWindow(env: CompilationEnvironment,
                                              groupExpr: GroupBy,
                                              windowExpr: TimeWindowExpression): KeyedStream[_, _] = {
    val inputStream = this.getOrCompileDataStream(env, groupExpr.source)
    val inputRecordType = groupExpr.getInputRecordType

    val eventTimeStream = FlinkWindowedStreamFactory.applyEventTime(windowExpr, inputStream)
    FlinkKeyedStreamFactory.keyStreamByFunction(inputRecordType, eventTimeStream, groupExpr.expr)
  }

  /**
   * Compiles a mapped stream that is the result of a grouping operation followed by a uniqueness constraint
   * followed by a select statement.
   */
  private def compileUniqueGroupSelect(env: CompilationEnvironment,
                                       mapExpr: MapNodeExpression): SingleOutputStreamOperator[_] = {
    val windowResult = this.getOrCompileWindowedStream(env, mapExpr.source.asInstanceOf[GroupingExpression])

    windowResult match {
      case GroupByWindowResult(result) =>
        FlinkAggregateUniqueFunctionFactory.applySelectToWindowedStream(
          mapExpr,
          result.windowedStream,
          result.windowTypeName)

      case UnkeyedTimeWindowResult(result) =>
        FlinkAggregateUniqueFunctionFactory.applySelectToAllWindowedStream(
          mapExpr,
          result.windowedStream,
          FlinkTypeNames.timeWindow)

      case KeyedTimeWindowResult(result) =>
        FlinkAggregateUniqueFunctionFactory.applySelectToWindowedStream(
          mapExpr,
          result.windowedStream,
          result.windowTypeName)
    }
  }

  /**
   * Compiles a [[GraphNodeExpression]] into a Flink windowed stream.
   */
  @tailrec
  private def compileWindowedStream(env: CompilationEnvironment, streamExpr: GraphNodeExpression): ApplyWindowResult = {
    streamExpr match {
      case g: GroupBy =>
        val inputStream = this.getOrCompileDataStream(env, g.source)
        GroupByWindowResult(FlinkWindowedStreamFactory.applyGroupByWindow(g, inputStream))

      case w: TimeWindowExpression =>
        w.source match {
          case g: GroupBy =>
            val keyedStream = this.compileKeyedStreamForTimeWindow(env, g, w)
            KeyedTimeWindowResult(FlinkWindowedStreamFactory.applyTimeWindow(w, keyedStream))

          case s: StreamExpression =>
            val inputStream = this.getOrCompileDataStream(env, s)
            UnkeyedTimeWindowResult(FlinkWindowedStreamFactory.applyTimeWindow(w, inputStream))
        }

      case UniqueBy(source, _) =>
        this.compileWindowedStream(env, source)

      case o =>
        throw new FlinkCompilationException(s"CompileWindowedStream: stream with ID '${streamExpr.nodeId}' has unsupported stream type '${o.getClass.getSimpleName}'.")
    }
  }

  /**
   * Compiles a [[MapNodeExpression]] that is the result of a map operation on an object or tuple stream.
   */
  private def compileMappedDataStream(env: CompilationEnvironment,
                                      mapExpr: MapNodeExpression): SingleOutputStreamOperator[_] = {
    val inputDataStream = this.getOrCompileDataStream(env, mapExpr.source.asInstanceOf[StreamExpression])

    val lineageFactory = new ComponentLineageRecordFactory(
      mapExpr.source.nodeId,
      env.applicationInstanceId,
      mapExpr.nodeId,
      mapExpr.nodeId,
      SemanticVersion.ZERO)

    val metricsFactory = new OperatorMetricFactory(env.config.metricPrefix, mapExpr.nodeName)

    val mappedWithLineage = FlinkMapFunctionFactory.applyMapFunction(
      mapExpr,
      inputDataStream,
      metricsFactory,
      lineageFactory)

    this.splitDataAndLineageStreams(env, mappedWithLineage)
  }

  /**
   * Compiles a [[MapNodeExpression]] that is the result of a join operation followed by a select statement.
   */
  private def compileJoinSelect(env: CompilationEnvironment,
                                mapExpr: MapNodeExpression): SingleOutputStreamOperator[_] = {
    val joinExpr = mapExpr.source.asInstanceOf[JoinNodeExpression]
    val connectedStreams = this.getOrCompileConnectedStream(env, joinExpr)
    val lineageFactory = new ComponentJoinLineageRecordFactory(
      joinExpr.left.nodeId,
      joinExpr.right.nodeId,
      env.applicationInstanceId,
      mapExpr.nodeId,
      mapExpr.nodeId,
      SemanticVersion.ZERO)

    val metricFactory = new OperatorMetricFactory(env.config.metricPrefix, mapExpr.nodeName)

    val outputWithLineage =
      FlinkCoProcessFunctionFactory.applyCoProcessFunction(
        mapExpr,
        connectedStreams.connectedStreams,
        connectedStreams.unappliedConditions,
        lineageFactory,
        metricFactory)

    this.splitDataAndLineageStreams(env, outputWithLineage)
  }

  /**
   * Compiles a stream represented by a [[JoinNodeExpression]].
   *
   * @param env      The compilation environment.
   * @param joinExpr The join expression.
   * @return A [[ConnectStreamsResult]] containing the results of the compilation.
   */
  private def compileJoinedStreamWithCondition(env: CompilationEnvironment,
                                               joinExpr: JoinNodeExpression): ConnectStreamsResult = {
    this.logger.info(s"Compiling join with condition '$joinExpr'.")

    val leftDataStream = this.getOrCompileDataStream(env, joinExpr.left)
    val rightDataStream = this.getOrCompileDataStream(env, joinExpr.right)
    val metricFactory = new OperatorMetricFactory(env.config.metricPrefix, getStreamName(env, joinExpr))

    try {
      FlinkStreamConnector.keyByAndConnectStreams(
        joinExpr,
        leftDataStream,
        rightDataStream,
        metricFactory)
    }
    catch {
      case ex: Throwable =>
        throw new FlinkCompilationException(s"Error compiling stream '${joinExpr.nodeName}'. Stream expression: '$joinExpr'.", ex)
    }
  }

  /**
   * Splits a [[DataStream]] of records with associated lineage into separate streams of records and lineage records.
   * The record stream is returned while the lineage stream is stored in the compilation environment.
   *
   * @param dataStream The data stream to split.
   * @return The resulting record stream.
   */
  private def splitDataAndLineageStreams(env: CompilationEnvironment,
                                         dataStream: SingleOutputStreamOperator[RecordWithLineage[_]]): SingleOutputStreamOperator[_] = {
    val (recordStream, lineageStream) = LineageSplitterFactory.splitStream(dataStream)
    env.lineageStreams += lineageStream
    recordStream
  }

  /**
   * Compiles all metrics for a stream.
   *
   * @param env        The compilation environment.
   * @param dataStream The input data stream.
   * @param streamExpr The graph node expression representing the data stream.
   */
  private def compileMetrics(env: CompilationEnvironment,
                             dataStream: SingleOutputStreamOperator[_],
                             streamExpr: StreamExpression): Unit = {
    val metrics = env.config.getMetricsForNode(streamExpr.nodeId)
    val histogramDefinitions =
      metrics
        .filter(_.isInstanceOf[HistogramDefinition[_]])
        .map(_.asInstanceOf[HistogramDefinition[_]])
        .toList

    if (histogramDefinitions.nonEmpty) {
      UserMetricsFactory.compileHistograms(histogramDefinitions, dataStream, streamExpr)
    }
  }

  private def getStreamName(env: CompilationEnvironment, expr: GraphNodeExpression): String = {
    implicit class StreamNameInterpolator(sc: StringContext) {
      def n(subs: Any*): String = {
        val partsIterator = sc.parts.iterator
        val subsIterator = subs.iterator

        val sb = new StringBuilder(partsIterator.next())
        while (subsIterator.hasNext) {
          sb.append(getStreamName(env, subsIterator.next().asInstanceOf[GraphNodeExpression]))
          sb.append(partsIterator.next())
        }

        sb.toString()
      }
    }

    expr match {
      case Ref(nodeId) =>
        val stream = env.graph.getStream(nodeId)
        if (stream.getExpression == expr) {
          stream.name
        }
        else {
          n"${stream.getExpression}"
        }

      case Filter(source, _) =>
        n"Filter $source"

      case FullJoin(left, right, _) =>
        n"FullEnrichmentJoin [$left] with [$right]"

      case LeftJoin(left, right, _) =>
        n"LeftEnrichmentJoin [$left] with [$right]"

      case MapNodeExpression(source) =>
        n"Map $source"

      case GroupBy(source, _) =>
        n"Group [$source]"

      case UniqueBy(source, _) =>
        n"UniqueBy [$source]"

      case _ =>
        throw new IllegalArgumentException(s"Unsupported stream expression: $expr")
    }
  }

  private class CompilationEnvironment(val applicationInstanceId: String,
                                       val graph: StreamGraph,
                                       val config: FlinkApplicationConfiguration,
                                       val streamEnvironment: StreamExecutionEnvironment) {
    val compiledDataStreams = new mutable.HashMap[String, SingleOutputStreamOperator[_]]()
    val compiledConnectedStreams = new mutable.HashMap[JoinNodeExpression, ConnectStreamsResult]()
    val compiledWindowedStreams = new mutable.HashMap[GroupingExpression, ApplyWindowResult]()
    val lineageStreams = new mutable.MutableList[DataStream[LineageRecord]]()
    val compiledMetrics = new mutable.MutableList[CompiledMetric]()
  }

  private trait ApplyWindowResult

  private case class UnkeyedTimeWindowResult(result: ApplyTimeWindowResult[_]) extends ApplyWindowResult

  private case class KeyedTimeWindowResult(result: ApplyGroupByWindowResult[_, _, TimeWindow]) extends ApplyWindowResult

  private case class GroupByWindowResult(result: ApplyGroupByWindowResult[_, _, _ <: Window]) extends ApplyWindowResult

}
