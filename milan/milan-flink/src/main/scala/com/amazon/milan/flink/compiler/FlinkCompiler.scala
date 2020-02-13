package com.amazon.milan.flink.compiler

import java.io.InputStream

import com.amazon.milan.SemanticVersion
import com.amazon.milan.application.metrics.HistogramDefinition
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.aws.metrics.CompiledMetric
import com.amazon.milan.flink.application.{FlinkApplicationConfiguration, FlinkApplicationInstance, FlinkDataSink}
import com.amazon.milan.flink.compiler.internal.FlinkWindowedStreamFactory.{ApplyKeyedWindowResult, ApplyUnkeyedWindowResult, ApplyWindowResult}
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

    env.graph.getStreams.foreach(stream => this.ensureStreamIsCompiled(env, stream))

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
  private def getOrCompileConnectedStream(env: CompilationEnvironment, joinExpr: Filter): ConnectStreamsResult = {
    env.compiledConnectedStreams.getOrElseUpdate(joinExpr, compileConnectedStream(env, joinExpr))
  }

  /**
   * Gets a compiled [[ApplyWindowResult]] for an expression, compiling it if necessary.
   *
   * @param env       The compilation environment.
   * @param groupExpr The expression whose compiled [[ApplyWindowResult]] will be returned.
   * @return An [[ApplyKeyedWindowResult]] containing the compiled [[ApplyWindowResult]] corresponding to the graph node.
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
    val recordTypeName = stream.getRecordTypeName

    val streamSink =
      this.eval.evalFunction[DataStream[_], SinkFunction[_], DataStreamSink[_]](
        "stream",
        FlinkTypeNames.dataStream(recordTypeName),
        "sinkFunction",
        FlinkTypeNames.sinkFunction(recordTypeName),
        "stream.addSink(sinkFunction)",
        dataStream,
        sink.getSinkFunction)

    streamSink.name(stream.nodeName)
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

        case e: ExternalStream =>
          this.compileExternalStream(env, e)

        case f: Filter =>
          this.compileFilteredStream(env, f)

        case m: MapExpression =>
          compileMappedStream(env, m)

        case m: FlatMapExpression =>
          compileFlatMappedStream(env, m)
      }

    this.compileMetrics(env, compiledStream, streamExpr)

    compiledStream
  }

  /**
   * Compiles a [[Filter]] that consumes a [[JoinExpression]] into a Flink [[ConnectedStreams]].
   *
   * @return A [[ConnectStreamsResult]] that contains the compiled [[ConnectedStreams]] object and a Milan function
   *         containing the portion of the join condition expression that must be applied in the Flink
   *         [[CoProcessFunction]].
   */
  private def compileConnectedStream(env: CompilationEnvironment, joinExpr: Filter): ConnectStreamsResult = {
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

    val uid = s"Source for ${streamExpr.nodeId}"
    source.addDataSource(env.streamEnvironment).name(streamExpr.nodeName).uid(uid)
  }

  /**
   * Compiles a stream with an external source.
   *
   * @param env        The compilation environment.
   * @param streamExpr The expression to compile.
   * @return A [[SingleOutputStreamOperator]] representing the stream in the Flink application.
   */
  private def compileExternalStream(env: CompilationEnvironment, streamExpr: ExternalStream): SingleOutputStreamOperator[_] = {
    this.logger.info(s"Compiling ExternalStream '${streamExpr.nodeName}'.")
    val source = env.config.getSource(streamExpr.nodeId)

    val uid = s"Source for ${streamExpr.nodeId}"
    source.addDataSource(env.streamEnvironment).name(streamExpr.nodeName).uid(uid)
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
   * Compiles a [[MapExpression]] into a Flink [[SingleOutputStreamOperator]].
   */
  private def compileMappedStream(env: CompilationEnvironment, expr: MapExpression): SingleOutputStreamOperator[_] = {
    expr.source match {
      case _: Filter =>
        compileJoinSelect(env, expr)

      case _: UniqueBy =>
        compileUniqueGroupSelect(env, expr)

      case _: GroupingExpression =>
        compileGroupSelect(env, expr)

      case _ =>
        compileMappedDataStream(env, expr)
    }
  }

  private def compileFlatMappedStream(env: CompilationEnvironment, expr: FlatMapExpression): SingleOutputStreamOperator[_] = {
    expr.source match {
      case _: LeftJoin =>
        this.compileWindowedJoin(env, expr)
    }
  }

  /**
   * Compiles a mapped stream that is the result of a windowed join followed by an apply statement.
   */
  private def compileWindowedJoin(env: CompilationEnvironment, mapExpr: FlatMapExpression): SingleOutputStreamOperator[_] = {
    mapExpr match {
      case FlatMap(LeftJoin(left, LatestBy(right, _, _)), _) =>

        val rightDataStream = this.getOrCompileDataStream(env, right)
        val leftDataStream = this.getOrCompileDataStream(env, left)

        val lineageRecordFactory = new ComponentJoinLineageRecordFactory(
          left.nodeId,
          right.nodeId,
          env.applicationInstanceId,
          mapExpr.nodeId,
          mapExpr.nodeId,
          SemanticVersion.ZERO)

        val outputWithLineage = FlinkWindowedJoinFactory.applyLatestByThenApply(
          mapExpr.asInstanceOf[FlatMap],
          leftDataStream,
          rightDataStream,
          lineageRecordFactory)

        this.splitDataAndLineageStreams(env, outputWithLineage)

      case unsupported =>
        throw new FlinkCompilationException(s"Cannot compile windowed join for expression '$unsupported'.")
    }
  }

  /**
   * Compiles a mapped stream that is the result of a grouping operation followed by a select statement.
   */
  private def compileGroupSelect(env: CompilationEnvironment,
                                 mapExpr: MapExpression): SingleOutputStreamOperator[_] = {
    val windowResult = this.getOrCompileWindowedStream(env, mapExpr.source.asInstanceOf[GroupingExpression])

    windowResult match {
      case ApplyKeyedWindowResult(windowedStream, windowTypeName) =>
        FlinkAggregateFunctionFactory.applySelectToWindowedStream(
          mapExpr,
          windowedStream,
          windowTypeName)

      case ApplyUnkeyedWindowResult(windowedStream) =>
        FlinkAggregateFunctionFactory.applySelectToAllWindowedStream(
          mapExpr,
          windowedStream,
          FlinkTypeNames.timeWindow)
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
                                       mapExpr: MapExpression): SingleOutputStreamOperator[_] = {
    val windowResult = this.getOrCompileWindowedStream(env, mapExpr.source.asInstanceOf[GroupingExpression])

    windowResult match {
      case ApplyKeyedWindowResult(windowedStream, windowTypeName) =>
        FlinkAggregateUniqueFunctionFactory.applySelectToWindowedStream(
          mapExpr,
          windowedStream,
          windowTypeName)

      case ApplyUnkeyedWindowResult(windowedStream) =>
        FlinkAggregateUniqueFunctionFactory.applySelectToAllWindowedStream(
          mapExpr,
          windowedStream,
          FlinkTypeNames.timeWindow)
    }
  }

  /**
   * Compiles a [[StreamExpression]] into a Flink windowed stream.
   */
  @tailrec
  private def compileWindowedStream(env: CompilationEnvironment, streamExpr: StreamExpression): ApplyWindowResult = {
    streamExpr match {
      case g: GroupBy =>
        val inputStream = this.getOrCompileDataStream(env, g.source)
        FlinkWindowedStreamFactory.applyGroupByWindow(g, inputStream)

      case w: TimeWindowExpression =>
        w.source match {
          case g: GroupBy =>
            val keyedStream = this.compileKeyedStreamForTimeWindow(env, g, w)
            FlinkWindowedStreamFactory.applyTimeWindow(w, keyedStream)

          case s: StreamExpression =>
            val inputStream = this.getOrCompileDataStream(env, s)
            FlinkWindowedStreamFactory.applyTimeWindow(w, inputStream)
        }

      case UniqueBy(source, _) =>
        this.compileWindowedStream(env, source)

      case o =>
        throw new FlinkCompilationException(s"CompileWindowedStream: stream with ID '${streamExpr.nodeId}' has unsupported stream type '${o.getClass.getSimpleName}'.")
    }
  }

  /**
   * Compiles a [[MapExpression]] that is the result of a map operation on an object or tuple stream.
   */
  private def compileMappedDataStream(env: CompilationEnvironment,
                                      mapExpr: MapExpression): SingleOutputStreamOperator[_] = {
    val inputDataStream = this.getOrCompileDataStream(env, mapExpr.source.asInstanceOf[StreamExpression])

    val lineageFactory = new ComponentLineageRecordFactory(
      mapExpr.source.asInstanceOf[StreamExpression].nodeId,
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
   * Compiles a [[MapExpression]] that is the result of a join operation followed by a select statement.
   */
  private def compileJoinSelect(env: CompilationEnvironment,
                                mapExpr: MapExpression): SingleOutputStreamOperator[_] = {
    val filterExpr = mapExpr.source.asInstanceOf[Filter]
    val JoinExpression(left, right) = filterExpr.source

    val connectedStreams = this.getOrCompileConnectedStream(env, filterExpr)
    val lineageFactory = new ComponentJoinLineageRecordFactory(
      left.nodeId,
      right.nodeId,
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
   * Compiles a stream represented by a [[JoinExpression]].
   *
   * @param env      The compilation environment.
   * @param joinExpr The join expression contained in the join condition.
   * @return A [[ConnectStreamsResult]] containing the results of the compilation.
   */
  private def compileJoinedStreamWithCondition(env: CompilationEnvironment,
                                               joinExpr: Filter): ConnectStreamsResult = {
    this.logger.info(s"Compiling join with condition '$joinExpr'.")

    val JoinExpression(left, right) = joinExpr.source

    val leftDataStream = this.getOrCompileDataStream(env, left)
    val rightDataStream = this.getOrCompileDataStream(env, right)
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

  private def getStreamName(env: CompilationEnvironment, expr: StreamExpression): String = {
    implicit class StreamNameInterpolator(sc: StringContext) {
      def n(subs: Any*): String = {
        val partsIterator = sc.parts.iterator
        val subsIterator = subs.iterator

        val sb = new StringBuilder(partsIterator.next())
        while (subsIterator.hasNext) {
          sb.append(getStreamName(env, subsIterator.next().asInstanceOf[StreamExpression]))
          sb.append(partsIterator.next())
        }

        sb.toString()
      }
    }

    expr match {
      case Ref(nodeId) =>
        val stream = env.graph.getStream(nodeId)
        if (stream == expr) {
          stream.nodeName
        }
        else {
          n"$stream"
        }

      case ExternalStream(_, nodeName, _) =>
        nodeName

      case Filter(FullJoin(left, right), _) =>
        n"FullEnrichmentJoin [$left] with [$right]"

      case LatestBy(source, _, _) =>
        n"LatestBy [$source]"

      case Filter(LeftJoin(left, right), _) =>
        n"LeftEnrichmentJoin [$left] with [$right]"

      case Filter(source, _) =>
        n"Filter $source"

      case MapExpression(source) =>
        n"Map $source"

      case FlatMapExpression(source) =>
        n"FlatMap $source"

      case GroupBy(source, _) =>
        n"Group [$source]"

      case UniqueBy(source, _) =>
        n"UniqueBy [$source]"

      case LeftJoin(left, right) =>
        n"LeftJoin [$left] with [$right]"

      case FullJoin(left, right) =>
        n"FullJoin [$left] with [$right]"

      case _ =>
        throw new IllegalArgumentException(s"Unsupported stream expression: $expr")
    }
  }

  private class CompilationEnvironment(val applicationInstanceId: String,
                                       val graph: StreamGraph,
                                       val config: FlinkApplicationConfiguration,
                                       val streamEnvironment: StreamExecutionEnvironment) {
    val compiledDataStreams = new mutable.HashMap[String, SingleOutputStreamOperator[_]]()
    val compiledConnectedStreams = new mutable.HashMap[Filter, ConnectStreamsResult]()
    val compiledWindowedStreams = new mutable.HashMap[GroupingExpression, ApplyWindowResult]()
    val lineageStreams = new mutable.MutableList[DataStream[LineageRecord]]()
    val compiledMetrics = new mutable.MutableList[CompiledMetric]()
  }

}
