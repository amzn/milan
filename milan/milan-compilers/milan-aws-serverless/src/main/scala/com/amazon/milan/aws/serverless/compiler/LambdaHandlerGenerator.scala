package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.ApplicationInstance
import com.amazon.milan.application.sources.{DynamoDbStreamSource, SqsDataSource}
import com.amazon.milan.aws.serverless.runtime.{EventSourceArnAccessor, MilanLambdaHandler, EnvironmentEventSourceArnAccessor}
import com.amazon.milan.compiler.scala._
import com.amazon.milan.compiler.scala.event.{EventHandlerClassGenerator, GeneratedStreams}
import com.amazon.milan.graph.{FlowGraph, StreamCollection}
import com.amazon.milan.program.StreamExpression
import com.amazon.milan.serialization.MilanObjectMapper


/**
 * Generates classes for the AWS serverless compiler.
 */
class LambdaHandlerGenerator {
  val typeLifter = new TypeLifter(new DefaultTypeEmitter)

  import typeLifter._

  /**
   * Generates the contents of the source file containing a lambda handler class and a Milan application compiled
   * to an event handler class.
   *
   * @param application            The Milan application instance to compile.
   * @param lambdaHandlerClassName The name of the lambda handler class to generate.
   * @return A string containing the definition of the lambda handler class and the Milan application.
   */
  def generateLambdaHandlerClass(application: ApplicationInstance,
                                 lambdaHandlerClassName: String = "EventHandler"): String = {
    val eventHandlerClassName = "ApplicationEventHandler"
    val eventHandlerClass = EventHandlerClassGenerator.generateClass(application, eventHandlerClassName)

    val context = this.createGeneratorContext(application, eventHandlerClass.generatedStreams)

    val streams = application.application.streams
    val lambdaHandlerClassDef = this.generateLambdaHandlerClass(context, lambdaHandlerClassName, streams, eventHandlerClassName)

    s"""$lambdaHandlerClassDef
       |
       |${eventHandlerClass.classDefinition}
       |""".stripMargin
  }

  /**
   * Generates the definition of a class that implements a lambda RequestStreamHandler for a Milan application.
   *
   * @param context               The generator context.
   * @param className             The name of the class to generate.
   * @param streams               The application streams.
   * @param eventHandlerClassName The name of the class that implements the Milan application.
   * @return A string containing the class definition for the lambda handler.
   */
  def generateLambdaHandlerClass(context: AwsGeneratorContext,
                                 className: String,
                                 streams: StreamCollection,
                                 eventHandlerClassName: String): String = {
    val eventHandlerClassVal = ValName("eventHandler")

    val graph = FlowGraph.build(streams.streams)
    val inputStreams = graph.rootNodes.map(_.expr)

    val inputEventSourceArnPrefixVals = this.generateInputEventSourceArnPrefixVals(inputStreams)
    val handleDynamoDbNewImageMethod = this.generateHandleNewImageMethod(context, eventHandlerClassVal, inputStreams)
    val handleSqsBodyMethod = this.generateHandleSqsBodyMethod(context, eventHandlerClassVal, inputStreams)

    qc"""
        |class ${code(className)}(environment: ${nameOf[EventSourceArnAccessor]}) extends ${nameOf[MilanLambdaHandler]}(environment) {
        |  ${inputEventSourceArnPrefixVals.indentTail(1)}
        |
        |  private val $eventHandlerClassVal = new ${code(eventHandlerClassName)}
        |
        |  def this() {
        |    this(new ${nameOf[EnvironmentEventSourceArnAccessor]})
        |  }
        |
        |  ${handleDynamoDbNewImageMethod.indentTail(1)}
        |
        |  ${handleSqsBodyMethod.indentTail(1)}
        |}
        |""".toString
  }

  /**
   * Creates the [[AwsGeneratorContext]] for an application instance.
   *
   * @param application                  An application instance.
   * @param eventHandlerGeneratedStreams The [[GeneratedStreams]] object containing information about the compiled
   *                                     application.
   * @return The [[AwsGeneratorContext]] for the application.
   */
  private def createGeneratorContext(application: ApplicationInstance,
                                     eventHandlerGeneratedStreams: GeneratedStreams): AwsGeneratorContext = {
    val eventSources =
      application.config.dataSources.map(source => {
        source.source match {
          case ddb: DynamoDbStreamSource[_] =>
            StreamEventSource("aws:dynamodb", source.streamId, ddb.tableName)

          case sqs: SqsDataSource[_] =>
            StreamEventSource("aws:sqs", source.streamId, sqs.queueArn)
        }
      })

    AwsGeneratorContext(application, eventSources, eventHandlerGeneratedStreams)
  }

  /**
   * Generates the class fields that contain event source ARN prefixes for the input streams.
   */
  private def generateInputEventSourceArnPrefixVals(streams: List[StreamExpression]): CodeBlock = {
    val fieldDefs =
      streams.map(stream => {
        val valName = this.getEventSourceArnPrefixFieldName(stream.nodeId)
        qc"private val $valName = this.getEventSourceArnPrefix(${stream.nodeName})"
      })

    qc"//$fieldDefs"
  }

  /**
   * Generates the body of the handleSqsBody method for the lambda handler class.
   */
  private def generateHandleSqsBodyMethod(context: AwsGeneratorContext,
                                          eventHandlerClassVal: ValName,
                                          inputStreams: List[StreamExpression]): CodeBlock = {
    val eventSourceArnVal = ValName("eventSourceArn")
    val bodyVal = ValName("body")

    val methodBody = this.generateHandlerMethodBody(
      context,
      inputStreams,
      "aws:sqs",
      eventHandlerClassVal,
      eventSourceArnVal,
      bodyVal
    )

    qc"""
        |override def ${MilanLambdaHandler.handleSqsBodyMethodName}($eventSourceArnVal: String, $bodyVal: String): Unit = {
        |  ${methodBody.indentTail(1)}
        |}
        |"""
  }

  /**
   * Generates the body of the handleDynamoDbNewImage method for the lambda handler class.
   */
  private def generateHandleNewImageMethod(context: AwsGeneratorContext,
                                           eventHandlerClassVal: ValName,
                                           inputStreams: List[StreamExpression]): CodeBlock = {
    val eventSourceArnVal = ValName("eventSourceArn")
    val newImageJsonVal = ValName("newImageJson")

    val methodBody = this.generateHandlerMethodBody(
      context,
      inputStreams,
      "aws:dynamodb",
      eventHandlerClassVal,
      eventSourceArnVal,
      newImageJsonVal
    )

    qc"""
        |override def ${MilanLambdaHandler.handleDynamoDbNewImageMethodName}($eventSourceArnVal: String, $newImageJsonVal: String): Unit = {
        |  ${methodBody.indentTail(1)}
        |}
        |"""
  }

  /**
   * Generates the body of a handler method.
   *
   * @param context              The generator context.
   * @param inputStreams         The input streams.
   * @param eventSource          The event source identifier, e.g. "aws:sqs" or "aws:dynamodb:.
   * @param eventHandlerClassVal The name of the value containing the compiled Milan application.
   * @param eventSourceArnVal    The name of the value containing the event source arn for the record being handled.
   * @param recordJsonVal        The name of the value containing the record.
   * @return A [[CodeBlock]] containing the generated method body.
   */
  private def generateHandlerMethodBody(context: AwsGeneratorContext,
                                        inputStreams: List[StreamExpression],
                                        eventSource: String,
                                        eventHandlerClassVal: ValName,
                                        eventSourceArnVal: ValName,
                                        recordJsonVal: ValName): CodeBlock = {
    val streamEventHandlerMethodNames =
      inputStreams
        .map(stream => stream.nodeId -> context.eventHandlerGeneratedStreams.getExternalStreamConsumer(stream.nodeId).consumerMethodName)
        .toMap

    val eventSources = context.eventSources.filter(_.eventSource == eventSource)

    val streamsById = inputStreams.map(s => s.nodeId -> s).toMap

    val dispatchers =
      eventSources.map(eventSource => {
        val methodName = streamEventHandlerMethodNames(eventSource.streamId)
        val stream = streamsById(eventSource.streamId)
        val recordType = stream.recordType
        val eventSourceArnPrefixField = this.getEventSourceArnPrefixFieldName(eventSource.streamId)

        qc"""
            |($eventSourceArnVal.startsWith(this.$eventSourceArnPrefixField)) {
            |  val record = ${nameOf[MilanObjectMapper]}.readValue[${recordType.toTerm}]($recordJsonVal, classOf[${recordType.toTerm}])
            |  $eventHandlerClassVal.$methodName(record)
            |}
            |"""
      })

    if (dispatchers.isEmpty) {
      CodeBlock.EMPTY
    }
    else {
      val firstDispatcher = qc"""if ${dispatchers.head}"""
      val tailDispatchers = dispatchers.tail.map(d => qc"""else if $d""")

      qc"""
          |${firstDispatcher.indentTail(1)}
          |//${tailDispatchers.map(_.indentTail(1))}
          |"""
    }
  }

  private def getEventSourceArnPrefixFieldName(streamId: String): ValName =
    ValName(toValidName(s"eventSourceArnPrefix$streamId"))
}
