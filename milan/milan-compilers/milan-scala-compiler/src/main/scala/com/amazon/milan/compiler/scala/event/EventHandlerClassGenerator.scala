package com.amazon.milan.compiler.scala.event

import com.amazon.milan.application.ApplicationInstance
import com.amazon.milan.compiler.scala._
import com.amazon.milan.graph._
import com.amazon.milan.program.{Aggregate, ExternalStream, FlatMap, GroupBy, InvalidProgramException, JoinExpression, ScanExpression, SelectTerm, SingleInputStreamExpression, SlidingRecordWindow, StreamExpression, StreamMap, Tree, TwoInputStreamExpression, WindowApply}
import com.amazon.milan.typeutil.{DataStreamTypeDescriptor, GroupedStreamTypeDescriptor, JoinedStreamsTypeDescriptor}

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 * Generates a Scala class that implements a Milan application.
 *
 * The class will have methods for handling input records from external streams.
 *
 * Input record handling is a blocking operation. As such, cycles are not supported by this generator.
 */
object EventHandlerClassGenerator {
  /*
  This generator works by turning each Milan operation into a class method that has an argument for each input stream.

  For each expression there is also a collector method in the class.
  When a record is sent to a collector it dispatches them to the downstream methods that consume those records.
   */

  private val typeLifter = new TypeLifter()
  private val componentGenerator = new EventHandlerFunctionGenerator(this.typeLifter)

  /**
   * Generates a Scala class that implements a Milan application.
   *
   * @param application A Milan application instance.
   * @return A string containing the definition of the generated class.
   */
  def generateClass(application: ApplicationInstance,
                    className: String): String = {
    val outputStream = new ByteArrayOutputStream()
    this.generateClass(application, className, outputStream)
    StandardCharsets.UTF_8.decode(ByteBuffer.wrap(outputStream.toByteArray)).toString
  }

  /**
   * Generates a Scala class that implements a Milan application.
   *
   * @param application  A Milan application instance.
   * @param outputStream An output stream where the class definition will be written.
   */
  def generateClass(application: ApplicationInstance,
                    className: String,
                    outputStream: OutputStream): Unit = {
    val streams = application.application.streams.getDereferencedStreams
    typeCheckGraph(streams)

    val dependencyGraph = DependencyGraph.build(streams)
    val flowGraph = FlowGraph.build(streams)
    val outputs = new GeneratorOutputs(this.typeLifter.typeEmitter)
    val plugins = EventHandlerGeneratorPlugin.loadAllPlugins(this.typeLifter)

    val context =
      GeneratorContext(
        application,
        outputs,
        ExpressionContext(Map.empty),
        dependencyGraph,
        flowGraph,
        plugins)

    val rootDataStreams = dependencyGraph.topologicalSort
      .filter(_.contextStream.isEmpty)
      .filter(_.expr.streamType.isInstanceOf[DataStreamTypeDescriptor])

    rootDataStreams.foreach(stream => this.getOrGenerateStream(context, stream.expr))

    outputs.generate(className, outputStream)
  }

  /**
   * Gets the [[StreamInfo]] corresponding to an expression, generating the expression implementation if it has not
   * already been generated.
   */
  def getOrGenerateStream(context: GeneratorContext,
                          expr: Tree): StreamInfo = {
    expr match {
      case SelectTerm(name) if context.expressionContext.streamTerms.contains(name) =>
        context.expressionContext.streamTerms(name)

      case streamExpr: StreamExpression =>
        context.outputs.getGeneratedStream(streamExpr.nodeId) match {
          case Some(stream) =>
            stream

          case None =>
            val stream = this.generateStream(context, streamExpr)
            context.outputs.addGeneratedStream(streamExpr.nodeId, stream)
            stream
        }

      case _ =>
        throw new UnexpectedExpressionException(s"Unexpected stream expression: $expr")
    }
  }

  /**
   * Generates the implementation of a [[StreamExpression]].
   */
  private def generateStream(context: GeneratorContext,
                             streamExpr: StreamExpression): StreamInfo = {
    val outputStream =
      streamExpr match {
        case mapExpr: StreamMap =>
          this.generateMap(context, mapExpr)

        case joinExpr: JoinExpression =>
          this.generateJoin(context, joinExpr)

        case groupByExpr: GroupBy =>
          this.generateGroupBy(context, groupByExpr)

        case flatMapExpr: FlatMap =>
          this.generateFlatMap(context, flatMapExpr)

        case scanExpr: ScanExpression =>
          this.generateScanExpression(context, scanExpr)

        case externalStream: ExternalStream =>
          this.componentGenerator.generateExternalStream(context.outputs, externalStream)

        case aggregateStream: Aggregate =>
          this.generateAggregate(context, aggregateStream)

        case windowApply: WindowApply =>
          this.generateWindowApply(context, windowApply)
      }

    // Generate the collector method for the stream, which is important because the generated stream handler method
    // will be calling this. We don't generate it first because we need to know the output stream type in order to
    // know the collector method signature.
    this.generateCollector(context, outputStream)

    outputStream
  }

  private def generateAggregate(context: GeneratorContext, aggregateExpr: Aggregate): StreamInfo = {
    aggregateExpr.source match {
      case window: SlidingRecordWindow =>
        val inputStream = this.getOrGenerateStream(context, window.source)
        if (inputStream.isKeyed) {
          this.componentGenerator.generateAggregateOfGroupedRecordWindow(context, inputStream, aggregateExpr, window)
        }
        else {
          throw new NotImplementedError()
        }

      case source =>
        val inputStream = this.getOrGenerateStream(context, source)
        this.componentGenerator.generateAggregateOfDataStream(context, inputStream, aggregateExpr)
    }
  }

  private def generateWindowApply(context: GeneratorContext, applyExpr: WindowApply): StreamInfo = {
    applyExpr.source match {
      case windowExpr: SlidingRecordWindow =>
        val inputStream = this.getOrGenerateStream(context, windowExpr.source)

        if (inputStream.isKeyed) {
          this.componentGenerator.generateApplyRecordWindowOfKeyedStream(context, inputStream, applyExpr, windowExpr)
        }
        else {
          throw new NotImplementedError()
        }

      case _ =>
        throw new NotImplementedError()
    }
  }

  /**
   * Generates the implementation of a [[ScanExpression]].
   */
  private def generateScanExpression(context: GeneratorContext, scanExpr: ScanExpression): StreamInfo = {
    val inputStream = this.getOrGenerateStream(context, scanExpr.source)
    this.componentGenerator.generateScan(context, inputStream, scanExpr)
  }

  /**
   * Generates the implementation of a [[GroupBy]] expression.
   */
  private def generateGroupBy(context: GeneratorContext, groupExpr: GroupBy): StreamInfo = {
    val inputStream = this.getOrGenerateStream(context, groupExpr.source)
    this.componentGenerator.generateGroupBy(context, inputStream, groupExpr)
  }

  /**
   * Generates the implementation of a [[FlatMap]] expression.
   */
  private def generateFlatMap(context: GeneratorContext, flatMapExpr: FlatMap): StreamInfo = {
    val inputStream = this.getOrGenerateStream(context, flatMapExpr.source)
    inputStream.streamType match {
      case _: GroupedStreamTypeDescriptor =>
        this.componentGenerator.generateFlatMapOfGroupedStream(context, inputStream, flatMapExpr)
    }
  }

  /**
   * Generates the implementation of a [[StreamMap]] expression.
   */
  private def generateMap(context: GeneratorContext, mapExpr: StreamMap): StreamInfo = {
    mapExpr.source.tpe match {
      case _: DataStreamTypeDescriptor =>
        this.generateMapDataStream(context, mapExpr)

      case _: GroupedStreamTypeDescriptor =>
        this.generateMapGroup(context, mapExpr)

      case _: JoinedStreamsTypeDescriptor =>
        this.generateJoinSelect(context, mapExpr)
    }
  }

  /**
   * Generates the implementation of a [[StreamMap]] expression that operates on a data stream.
   */
  private def generateMapDataStream(context: GeneratorContext, mapExpr: StreamMap): StreamInfo = {
    val inputStream = this.getOrGenerateStream(context, mapExpr.source)
    this.componentGenerator.generateStreamMap(context.outputs, inputStream, mapExpr)
  }

  /**
   * Generates the implementation of a [[StreamMap]] expression that operates on a grouping.
   */
  private def generateMapGroup(context: GeneratorContext, mapExpr: StreamMap): StreamInfo = {
    // When you map a grouping, you don't actually do anything.
    // You just apply the map function to the input stream while retaining the keyed properties of the input.
    val groupStreamTerm = mapExpr.expr.arguments(1).name
    val inputStream = this.getOrGenerateStream(context, mapExpr.source)
    val mapContext = context.withStreamTerm(groupStreamTerm, inputStream)

    // Perform the operations defined in the body of the map function.
    val mappedStream = this.getOrGenerateStream(mapContext, mapExpr.expr.body)

    mappedStream
  }

  /**
   * Generates the implementation of a [[StreamMap]] expression that operates on the output of a join expression.
   */
  private def generateJoinSelect(context: GeneratorContext, mapExpr: StreamMap): StreamInfo = {
    val joinedStream = this.getOrGenerateStream(context, mapExpr.source)
    this.componentGenerator.generateJoinSelect(context.outputs, joinedStream, mapExpr)
  }

  /**
   * Generates the implementation of a [[JoinExpression]] expression.
   */
  private def generateJoin(context: GeneratorContext, joinExpr: JoinExpression): StreamInfo = {
    val leftInputStream = this.getOrGenerateStream(context, joinExpr.left)
    val rightInputStream = this.getOrGenerateStream(context, joinExpr.right)
    this.componentGenerator.generateJoin(context, leftInputStream, rightInputStream, joinExpr)
  }

  /**
   * Gets a list of [[StreamConsumerInfo]] objects that describe consumers of the output of a stream.
   */
  private def getConsumers(context: GeneratorContext, provider: StreamInfo): List[StreamConsumerInfo] = {
    this.getStreamConsumers(context, provider.expr) ++ this.getSinkConsumers(context, provider)
  }

  /**
   * Gets a list of [[StreamConsumerInfo]] objects for the data sinks that consume a stream.
   */
  private def getSinkConsumers(context: GeneratorContext, provider: StreamInfo): List[StreamConsumerInfo] = {
    context.application.config.dataSinks.filter(_.streamId == provider.streamId)
      .map(sink => this.componentGenerator.generateDataSink(context, provider, sink.sink))
  }

  /**
   * Gets a list of [[StreamConsumerInfo]] objects for the streams that consume a stream.
   */
  private def getStreamConsumers(context: GeneratorContext, providerExpr: StreamExpression): List[StreamConsumerInfo] = {
    val consumerStreams = context.flowGraph.getDependentExpressions(providerExpr)

    consumerStreams.flatMap {
      case windowExpr: SlidingRecordWindow =>
        // SlidingRecordWindow expressions don't consume records directly, they send them on to their downstream
        // consumers.
        this.getStreamConsumers(context, windowExpr)

      case groupMapExpr@StreamMap(GroupBy(_, _), _) =>
        // StreamMap of GroupBy expressions don't consume records directly, they send them on to their downstream
        // consumers.
        this.getStreamConsumers(context, groupMapExpr)

      case expr@TwoInputStreamExpression(left, right) =>
        if (left == providerExpr) {
          List(StreamConsumerInfo(expr.nodeName, "left"))
        }
        else if (right == providerExpr) {
          List(StreamConsumerInfo(expr.nodeName, "right"))
        }
        else {
          throw new InvalidProgramException(s"Upstream expression $providerExpr wasn't found in the inputs of $expr.")
        }

      case expr: SingleInputStreamExpression =>
        List(StreamConsumerInfo(expr.nodeName, "input"))

      case expr =>
        throw new InvalidProgramException(s"Unsupported stream expression: $expr")
    }
  }

  /**
   * Generates a class method that is the collector for a given stream expression.
   *
   * @param context  The generator context.
   * @param provider The stream to generate a collector for.
   * @return The name of the generated method.
   */
  private def generateCollector(context: GeneratorContext,
                                provider: StreamInfo): MethodName = {
    val consumers = this.getConsumers(context, provider)
    this.componentGenerator.generateCollector(context.outputs, provider, consumers)
  }
}
