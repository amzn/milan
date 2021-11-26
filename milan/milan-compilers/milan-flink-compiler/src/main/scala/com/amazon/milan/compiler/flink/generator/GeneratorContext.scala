package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.application.ApplicationConfiguration.StreamSink
import com.amazon.milan.program.{Aggregate, Cycle, ExternalStream, Filter, FlatMap, GroupBy, GroupingExpression, JoinExpression, Last, LeftWindowedJoin, ScanExpression, SelectTerm, SlidingRecordWindow, StreamExpression, StreamMap, TimeWindowExpression, Tree, Union, WindowApply}
import com.amazon.milan.typeutil.{DataStreamTypeDescriptor, GroupedStreamTypeDescriptor}


object GeneratorContext {
  def createEmpty(applicationInstanceId: String,
                  appConfig: ApplicationConfiguration,
                  output: GeneratorOutputs,
                  typeLifter: FlinkTypeLifter) =
    new GeneratorContext(applicationInstanceId, appConfig, output, typeLifter, Map.empty)
}


/**
 * Encapsulates the context for a Flink code generation operation.
 */
class GeneratorContext(val applicationInstanceId: String,
                       val appConfig: ApplicationConfiguration,
                       val output: GeneratorOutputs,
                       contextTypeLifter: FlinkTypeLifter,
                       streamTerms: Map[String, GeneratedStream]) {

  private val components =
    new Object
      with DataSinkGenerator
      with DataSourceGenerator
      with ConnectedStreamsGenerator
      with CoProcessFunctionGenerator
      with MapFunctionGenerator
      with WindowedStreamsGenerator
      with AggregateFunctionGenerator
      with FilteredStreamGenerator
      with FlatMapGenerator
      with KeyedStreamGenerator
      with ScanExpressionGenerator
      with RecordWindowGenerator
      with LastByGenerator {
      val typeLifter: FlinkTypeLifter = contextTypeLifter
    }

  /**
   * Gets a new [[GeneratorContext]] that contains the information in the current context plus term that refers to a
   * generated stream.
   *
   * @param termName A term name.
   * @param stream   The generated stream referenced by the term.
   * @return A [[GeneratorContext]] containing the information from this context plus the specified stream term.
   */
  def withStreamTerm(termName: String, stream: GeneratedStream): GeneratorContext = {
    new GeneratorContext(
      this.applicationInstanceId,
      this.appConfig,
      this.output,
      this.contextTypeLifter,
      this.streamTerms + (termName -> stream))
  }

  /**
   * Adds a data sink to the generated program.
   *
   * @param sink The sink to generate.
   */
  def generateSink(sink: StreamSink): Unit = {
    val stream = this.output.dataStreams(sink.streamId)
    this.components.addDataSink(this.output, stream, sink.sink)
  }

  /**
   * Gets the [[GeneratedStream]] that was created when an expression was generated.
   * If no [[GeneratedStream]] is associated with the expression then an exception is thrown.
   *
   * @param expr An expression.
   * @return The [[GeneratedStream]] corresponding to the expression.
   */
  def getGeneratedStream(expr: Tree): GeneratedStream = {
    this.tryGetGeneratedStream(expr) match {
      case Some(stream) =>
        stream

      case None =>
        throw new FlinkGeneratorException(s"Unrecognized stream expression '$expr'.")
    }
  }

  def getOrGenerateDataStream(expr: Tree): GeneratedDataStream = {
    expr match {
      case streamExpr: StreamExpression =>
        this.output.dataStreams.getOrElseUpdate(streamExpr.nodeId, this.generateDataStream(streamExpr))

      case SelectTerm(name) if this.streamTerms.contains(name) =>
        this.streamTerms(name).asInstanceOf[GeneratedDataStream]

      case _ =>
        throw new FlinkGeneratorException(s"Unrecognized data stream expression '$expr'.")
    }
  }

  def getOrGenerateConnectedStreams(joinExpr: JoinExpression): GeneratedConnectedStreams = {
    this.output.connectedStreams.getOrElseUpdate(joinExpr.nodeId, this.generateConnectedStreams(joinExpr))
  }

  def getOrGenerateWindowedStream(expr: Tree): GeneratedGroupedStream = {
    expr match {
      case groupExpr: GroupingExpression =>
        this.output.windowedStreams.getOrElseUpdate(groupExpr.nodeId, this.generateGroupedStream(groupExpr))

      case SelectTerm(name) if this.streamTerms.contains(name) =>
        this.streamTerms(name).asInstanceOf[GeneratedGroupedStream]

      case mapExpr@StreamMap(groupExpr: GroupingExpression, _) =>
        this.generateMapGroup(mapExpr, groupExpr)

      case _ =>
        throw new FlinkGeneratorException(s"Unrecognized windowed stream expression '$expr'.")
    }
  }

  def closeCycle(cycleExpr: Cycle): Unit = {
    val iterationStreamVal = this.output.getGeneratedStreamVal(cycleExpr.nodeId)
    val cycleStreamVal = this.output.getGeneratedStreamVal(cycleExpr.cycleNodeId)

    val codeBlock = s"$iterationStreamVal.closeWith($cycleStreamVal)"

    this.output.setHasCyclesTrue()
    this.output.appendMain(codeBlock)
  }

  def getOrGenerateStream(expr: Tree): GeneratedStream = {
    (expr, expr.tpe) match {
      case (_, _: DataStreamTypeDescriptor) =>
        this.getOrGenerateDataStream(expr)

      case (joinExpr: JoinExpression, _) =>
        this.getOrGenerateConnectedStreams(joinExpr)

      case (_, _: GroupedStreamTypeDescriptor) =>
        this.getOrGenerateWindowedStream(expr)

      case _ =>
        throw new FlinkGeneratorException(s"Unable to generate stream with type '${expr.tpe}' and expression '$expr'.")
    }
  }

  private def tryGetGeneratedStream(expr: Tree): Option[GeneratedStream] = {
    expr match {
      case streamExpr: StreamExpression =>
        this.output.dataStreams.get(streamExpr.nodeId)
          .orElse(this.output.windowedStreams.get(streamExpr.nodeId))
          .orElse(this.output.connectedStreams.get(streamExpr.nodeId))

      case SelectTerm(name) if this.streamTerms.contains(name) =>
        this.streamTerms.get(name)

      case _ =>
        None
    }
  }

  private def generateDataStream(streamExpr: StreamExpression): GeneratedDataStream = {
    streamExpr match {
      case flatMapExpr: FlatMap =>
        this.generateFlatMapExpression(flatMapExpr)

      case aggExpr: Aggregate =>
        this.generateAggregateExpression(aggExpr)

      case mapExpr: StreamMap =>
        this.generateMapExpression(mapExpr)

      case applyExpr: WindowApply =>
        this.generateApplyWindow(applyExpr)

      case externalStream: ExternalStream =>
        this.generateExternalStream(externalStream)

      case filterExpr: Filter =>
        this.generateFilter(filterExpr)

      case scanExpr: ScanExpression =>
        this.generateScanExpression(scanExpr)

      case lastExpr: Last =>
        this.components.applyLastOperation(this, lastExpr)

      case unionExpr: Union =>
        this.generateUnion(unionExpr)

      case groupByExpr: GroupBy =>
        this.components.applyGroupBy(this, groupByExpr)

      case cycleExpr: Cycle =>
        this.generateBeginCycle(cycleExpr)
    }
  }

  private def generateGroupedStream(groupExpr: GroupingExpression): GeneratedGroupedStream = {
    groupExpr match {
      case groupByExpr: GroupBy =>
        this.components.applyGroupBy(this, groupByExpr)

      case windowExpr: TimeWindowExpression =>
        val inputStream = this.getOrGenerateDataStream(windowExpr.source)
        val eventTimeStream = this.components.applyEventTime(this.output, windowExpr, inputStream)
        this.components.applyTimeWindowToEventTimeStream(this.output, windowExpr, eventTimeStream)

      case o =>
        throw new FlinkGeneratorException(s"Stream with ID '${o.nodeId}' has unsupported stream type '${o.expressionType}' for a windowed stream.")
    }
  }

  private def generateConnectedStreams(joinExpr: JoinExpression): GeneratedConnectedStreams = {
    val leftInput = this.getOrGenerateDataStream(joinExpr.left)
    val rightInput = this.getOrGenerateDataStream(joinExpr.right)

    this.components.keyByAndConnectStreams(this.output, joinExpr, leftInput, rightInput)
  }

  private def generateExternalStream(streamExpr: ExternalStream): GeneratedUnkeyedDataStream = {
    val streamVal = this.output.newStreamValName(streamExpr)
    val source = this.appConfig.dataSources(streamExpr.nodeId)
    this.components.addDataSource(this.output, source, streamVal, streamExpr.nodeId)
  }

  private def generateFilter(filterExpr: Filter): GeneratedDataStream = {
    val source = this.getOrGenerateDataStream(filterExpr.source)
    this.components.applyFilter(this.output, source, filterExpr)
  }

  private def generateFlatMapExpression(flatMapExpr: FlatMap): GeneratedDataStream = {
    flatMapExpr.source match {
      case groupingExpr: GroupingExpression =>
        val windowedStreamResult = this.getOrGenerateWindowedStream(groupingExpr)
        this.components.applyFlatMap(this, windowedStreamResult, flatMapExpr)

      case leftWindowedJoin: LeftWindowedJoin =>
        val leftDataStream = this.getOrGenerateDataStream(leftWindowedJoin.left)
        val rightWindowedStream = this.getOrGenerateWindowedStream(leftWindowedJoin.right)
        this.components.applyFlatMap(this, leftDataStream, rightWindowedStream, flatMapExpr, leftWindowedJoin)
    }
  }

  private def generateAggregateExpression(aggExpr: Aggregate): GeneratedDataStream = {
    aggExpr.source match {
      case groupExpr: GroupingExpression =>
        this.generateGroupSelect(aggExpr, groupExpr)
    }
  }

  private def generateMapExpression(mapExpr: StreamMap): GeneratedDataStream = {
    mapExpr.source match {
      case joinExpr: JoinExpression =>
        this.generateJoinSelect(mapExpr, joinExpr)

      case sourceExpr: StreamExpression =>
        val inputStream = this.getOrGenerateDataStream(sourceExpr)
        this.components.applyMapExpression(this, mapExpr, inputStream)
    }
  }

  private def generateScanExpression(scanExpr: ScanExpression): GeneratedDataStream = {
    val source = this.getOrGenerateDataStream(scanExpr.source)
    this.components.applyScanExpression(this, scanExpr, source)
  }

  private def generateApplyWindow(applyExpr: WindowApply): GeneratedDataStream = {
    applyExpr.source match {
      case windowExpr: SlidingRecordWindow =>
        val inputStream = this.getOrGenerateStream(windowExpr.source)

        inputStream match {
          case keyedStream: GeneratedKeyedDataStream =>
            this.components.applyRecordWindowApplyOfKeyedStream(this, keyedStream, applyExpr, windowExpr)

          case _ =>
            throw new NotImplementedError()
        }

      case _ =>
        throw new NotImplementedError()
    }
  }

  private def generateMapGroup(mapExpr: StreamMap,
                               groupExpr: GroupingExpression): GeneratedGroupedStream = {
    val inputGroupedStream = this.getOrGenerateWindowedStream(groupExpr)

    inputGroupedStream match {
      case keyedDataStream: GeneratedKeyedDataStream =>
        this.components.applyMapGroup(this, mapExpr, keyedDataStream)
    }
  }

  private def generateGroupSelect(aggExpr: Aggregate,
                                  groupExpr: GroupingExpression): GeneratedUnkeyedDataStream = {
    val windowResult = this.getOrGenerateWindowedStream(groupExpr)

    windowResult match {
      case k: GeneratedKeyedDataStream =>
        val windowedStream = this.components.applyGlobalWindowToKeyedStream(this.output, k)
        this.components.applySelectToWindowedStream(this.output, aggExpr, windowedStream)

      case keyedWindow: GeneratedKeyedWindowedStream =>
        this.components.applySelectToWindowedStream(this.output, aggExpr, keyedWindow)

      case unkeyedWindow: GeneratedUnkeyedWindowStream =>
        this.components.applySelectToAllWindowedStream(this.output, aggExpr, unkeyedWindow)
    }
  }

  /**
   * Generates the code for a select from a join expression.
   *
   * @param mapExpr  The map expression representing the select.
   * @param joinExpr The join expression that is the input to the select.
   * @return The generated data stream.
   */
  private def generateJoinSelect(mapExpr: StreamMap,
                                 joinExpr: JoinExpression): GeneratedUnkeyedDataStream = {
    val connectedStreams = this.getOrGenerateConnectedStreams(joinExpr)
    val leftInputStream = this.getGeneratedStream(joinExpr.left)
    val rightInputStream = this.getGeneratedStream(joinExpr.right)

    this.components.applyKeyedCoProcessFunction(
      this,
      mapExpr,
      leftInputStream,
      rightInputStream,
      connectedStreams.keyType,
      connectedStreams.streamVal,
      connectedStreams.unappliedConditions)
  }

  private def generateUnion(unionExpr: Union): GeneratedUnkeyedDataStream = {
    val leftInputStream = this.getOrGenerateDataStream(unionExpr.left)
    val rightInputStream = this.getOrGenerateDataStream(unionExpr.right)

    val outputStreamVal = this.output.newStreamValName(unionExpr)

    val codeBlock = s"$outputStreamVal = ${leftInputStream.streamVal}.union(${rightInputStream.streamVal})"
    this.output.appendMain(codeBlock)

    GeneratedUnkeyedDataStream(unionExpr.nodeId, outputStreamVal, leftInputStream.recordType, leftInputStream.keyType, isContextual = false)
  }

  private def generateBeginCycle(cycleExpr: Cycle): GeneratedDataStream = {
    val inputStream = this.getOrGenerateDataStream(cycleExpr.source)
    val outputStreamVal = this.output.newStreamValName(cycleExpr)

    val codeBlock = s"val $outputStreamVal = ${inputStream.streamVal}.iterate()"
    this.output.appendMain(codeBlock)

    inputStream.withStreamVal(outputStreamVal)
  }
}
