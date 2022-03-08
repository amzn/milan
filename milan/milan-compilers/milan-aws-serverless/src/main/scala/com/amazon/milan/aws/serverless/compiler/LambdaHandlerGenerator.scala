package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.ApplicationConfiguration.StreamDataSource
import com.amazon.milan.application.ApplicationInstance
import com.amazon.milan.application.sources.{DynamoDbStreamSource, SqsDataSource}
import com.amazon.milan.aws.serverless.runtime.{EnvironmentAccessor, MilanLambdaHandler, SystemEnvironmentAccessor}
import com.amazon.milan.compiler.scala._
import com.amazon.milan.compiler.scala.event.{ClassProperty, EventHandlerClassGenerator, EventHandlerGeneratorPlugin, GeneratorOutputInfo}
import com.amazon.milan.graph.{FlowGraph, StreamCollection}
import com.amazon.milan.program.StreamExpression
import com.amazon.milan.serialization.MilanObjectMapper


case class GeneratedLambdaHandlerClass(classDef: String, handlerGeneratorOutput: GeneratorOutputInfo)


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
   * @param plugin                 The plugin for the code generator.
   * @param lambdaHandlerClassName The name of the lambda handler class to generate.
   * @return A string containing the definition of the lambda handler class and the Milan application.
   */
  def generateLambdaHandlerClass(application: ApplicationInstance,
                                 plugin: EventHandlerGeneratorPlugin,
                                 lambdaHandlerClassName: String = "EventHandler"): GeneratedLambdaHandlerClass = {
    val eventHandlerClassName = toValidIdentifier(s"${application.application.applicationId}EventHandler")
    val eventHandlerClass = EventHandlerClassGenerator.generateClass(application, eventHandlerClassName, Some(plugin))

    val context = this.createGeneratorContext(application, eventHandlerClass.generatorOutputInfo)

    val streams = application.application.streams
    val lambdaHandlerClassDef = this.generateLambdaHandlerClass(context, lambdaHandlerClassName, streams, eventHandlerClassName)

    val classDef =
      s"""$lambdaHandlerClassDef
         |
         |${eventHandlerClass.classDefinition}
         |""".stripMargin

    GeneratedLambdaHandlerClass(classDef, context.eventHandlerGeneratorOutputInfo)
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

    val environmentVal = ValName("environment")

    val handlerPropertiesVal = ValName("eventHandlerProperties")
    val createPropertiesInstance = this.generatePropertiesInstanceFromEnvironment(
      context.eventHandlerGeneratorOutputInfo,
      environmentVal
    )

    qc"""
        |class ${code(className)}($environmentVal: ${nameOf[EnvironmentAccessor]}) extends ${nameOf[MilanLambdaHandler]}(environment) {
        |  ${inputEventSourceArnPrefixVals.indentTail(1)}
        |
        |  private val $handlerPropertiesVal = ${createPropertiesInstance.indentTail(1)}
        |
        |  private val $eventHandlerClassVal = new ${code(eventHandlerClassName)}($handlerPropertiesVal)
        |
        |  def this() {
        |    this(new ${nameOf[SystemEnvironmentAccessor]})
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
   * @param application                     An application instance.
   * @param eventHandlerGeneratorOutputInfo The [[GeneratorOutputInfo]] object containing information about the compiled
   *                                        application.
   * @return The [[AwsGeneratorContext]] for the application.
   */
  private def createGeneratorContext(application: ApplicationInstance,
                                     eventHandlerGeneratorOutputInfo: GeneratorOutputInfo): AwsGeneratorContext = {
    val eventSources = application.config.dataSources.map(this.createEventSource)
    AwsGeneratorContext(application, eventSources, eventHandlerGeneratorOutputInfo)
  }

  /**
   * Creates a [[StreamEventSource]] corresponding to a [[StreamDataSource]].
   *
   * @param streamSource A [[StreamDataSource]] instance.
   * @return A [[StreamEventSource]] that describes the source of stream events.
   */
  private def createEventSource(streamSource: StreamDataSource): StreamEventSource = {
    val actualSource = toConcreteDataSource(streamSource.source)

    actualSource match {
      case _: DynamoDbStreamSource[_] =>
        StreamEventSource("aws:dynamodb", streamSource.streamId)

      case _: SqsDataSource[_] =>
        StreamEventSource("aws:sqs", streamSource.streamId)
    }
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
        .map(stream => stream.nodeId -> context.eventHandlerGeneratorOutputInfo.getExternalStreamConsumer(stream.nodeId).consumerMethodName)
        .toMap

    val eventSources = context.eventSources.filter(_.eventSource == eventSource)

    val streamsById = inputStreams.map(s => s.nodeId -> s).toMap

    // Gets the code that checks for a specific stream event source and calls the appropriate handler.
    def getDispatcherCode(eventSource: StreamEventSource): CodeBlock = {
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
    }

    val dispatchers = eventSources.map(getDispatcherCode)

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

  private def generatePropertiesInstanceFromEnvironment(eventHandlerGeneratorOutputInfo: GeneratorOutputInfo,
                                                        environmentVal: ValName): CodeBlock = {
    val propertyValues = eventHandlerGeneratorOutputInfo.properties.map(prop =>
      this.generatePropertyValueFromEnvironment(prop, environmentVal)
    )

    val propertiesList = qc"./$propertyValues"
    qc"new ${code(eventHandlerGeneratorOutputInfo.propertiesClassName)}($propertiesList)"
  }

  private def generatePropertyValueFromEnvironment(property: ClassProperty, environmentVal: ValName): CodeBlock = {
    qc"${code(property.propertyName)} = $environmentVal.getEnv(${property.propertyName})"
  }
}
