package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.ApplicationConfiguration.{StreamDataSource, StreamSink}
import com.amazon.milan.application.ApplicationInstance
import com.amazon.milan.application.sinks.DynamoDbTableSink
import com.amazon.milan.application.sources.DynamoDbStreamSource
import com.amazon.milan.aws.serverless.runtime.EnvironmentEventSourceArnAccessor
import com.amazon.milan.compiler.scala.{ClassName, CodeBlock, MethodName, ValName, toValidIdentifier, toValidName}


class LambdaCdkGenerator {

  val typeScriptLifter = new TypeScriptLifter

  import typeScriptLifter._

  /**
   * Generates a typescript file with method that creates a lambda function construct.
   *
   * @param applicationInstance The application instance executing in the generated lambda.
   * @param handlerPackage      The java package containing the lambda handler class.
   * @param cdkPackage          The name of the nodejs CDK package, e.g. "aws-cdk-lib".
   * @return A [[CodeBlock]] containing the contents of the file.
   */
  def generateLambdaDefinition(applicationInstance: ApplicationInstance,
                               jarPath: String,
                               handlerPackage: String,
                               handlerClassName: String,
                               cdkPackage: String): String = {
    val lambdaInfo = this.getLambdaInfo(applicationInstance, handlerPackage, handlerClassName)
    this.generateLambdaDefinition(
      applicationInstance,
      lambdaInfo,
      jarPath,
      cdkPackage,
      "lambda.Function")
  }

  /**
   * Generates a TypeScript class that defines a lambda function for an application instance.
   */
  def generateLambdaDefinition(applicationInstance: ApplicationInstance,
                               lambdaInfo: GeneratedLambdaInfo,
                               jarPath: String,
                               cdkPackage: String,
                               baseClassName: String): String = {
    val propsClass = this.generateLambdaPropsClass(lambdaInfo)
    val methodDef = this.generateLambdaConstructMethod(applicationInstance, lambdaInfo, jarPath, ClassName(baseClassName))

    val imports = this.generateImports(lambdaInfo, cdkPackage)

    qc"""$imports
        |
        |$propsClass
        |
        |$methodDef
        |""".toString
  }

  private def generateImports(lambdaInfo: GeneratedLambdaInfo, cdkPackage: String): CodeBlock = {
    val importStatements = (lambdaInfo.sourceConstructs.values.flatMap(_.importStatements).toSet ++
      lambdaInfo.sinkConstructs.values.flatMap(_.importStatements).toSet)
      .toList
      .map(_.replace("{cdk}", cdkPackage))
      .sorted
      .map(statement => CodeBlock(statement))

    qc"""import cdk = require($cdkPackage);
        |import lambda = require(${cdkPackage + "/aws-lambda"});
        |import { Construct } from "constructs";
        |//$importStatements
        |"""
  }

  private def generateLambdaPropsClass(lambdaInfo: GeneratedLambdaInfo): CodeBlock = {
    val sourceFields =
      lambdaInfo.sourceConstructs.values.map(info => qc"readonly ${info.propsField}: ${info.constructType};")
    val sourceFieldSnippet = qc"""//$sourceFields"""

    val sinkFields =
      lambdaInfo.sinkConstructs.values.map(info => qc"readonly ${info.propsField}: ${info.constructType};")
    val sinkFieldSnippet = qc"""//$sinkFields"""

    qc"""export interface ${lambdaInfo.propsClassName} {
        |  ${sourceFieldSnippet.indentTail(2)}
        |  ${sinkFieldSnippet.indentTail(2)}
        |  readonly timeout?: cdk.Duration;
        |  readonly memorySize?: number;
        |}
        |"""
  }

  private def generateLambdaConstructMethod(applicationInstance: ApplicationInstance,
                                            lambdaInfo: GeneratedLambdaInfo,
                                            jarPath: String,
                                            baseClassName: ClassName): CodeBlock = {
    val scopeVal = ValName("scope")
    val lambdaVal = ValName("lambdaFunction")
    val propsVal = ValName("props")

    val triggerCode = this.generateSourcesCode(applicationInstance, lambdaVal, scopeVal, propsVal, lambdaInfo)
    val sinkCode = this.generateSinkCode(applicationInstance, lambdaVal, propsVal, lambdaInfo)

    val environmentMap =
      lambdaInfo.sourceConstructs.map { case (streamId, construct) =>
        val envVar = EnvironmentEventSourceArnAccessor.getEventSourceArnEnvironmentVariableName(streamId)
        val arn = this.getEventSourceArnSnippet(qc"""$propsVal.${construct.propsField}""", construct)

        // We have to make sure the snippets evaluate to strings.
        envVar -> qc"""`$${$arn}`"""
      }

    val environmentCode = qc"$environmentMap"

    qc"""export function ${lambdaInfo.constructMethodName}($scopeVal: Construct, $propsVal: ${lambdaInfo.propsClassName}) {
        |  const $lambdaVal = new $baseClassName($scopeVal, ${lambdaInfo.constructId}, {
        |    functionName: ${lambdaInfo.functionName},
        |    handler: ${lambdaInfo.handler},
        |    runtime: lambda.Runtime.JAVA_8,
        |    code: lambda.Code.fromAsset($jarPath),
        |    environment: ${environmentCode.indentTail(2)},
        |    timeout: props.timeout,
        |    memorySize: props.memorySize,
        |  });
        |
        |  ${triggerCode.indentTail(1)}
        |
        |  ${sinkCode.indentTail(1)}
        |
        |  return $lambdaVal;
        |}
        |"""
  }

  /**
   * Gets a code snippet that extracts the event source ARN for a construct.
   *
   * @param constructReference A [[CodeBlock]] that resolves to a reference to the construct that represents the event
   *                           source.
   * @param construct          Information about the construct.
   * @return A code snippet that extracts the event source ARN.
   */
  private def getEventSourceArnSnippet(constructReference: CodeBlock, construct: SourceConstructInfo): CodeBlock = {
    construct.source.source match {
      case _: DynamoDbStreamSource[_] =>
        qc"""$constructReference.tableStreamArn"""
    }
  }

  private def generateSinkCode(applicationInstance: ApplicationInstance,
                               lambdaVal: ValName,
                               propsVal: ValName,
                               lambdaInfo: GeneratedLambdaInfo): CodeBlock = {
    val sinks = applicationInstance.config.dataSinks
    val sinkCodeBlocks = sinks.map(sink => this.generateSinkCode(lambdaVal, propsVal, sink, lambdaInfo))

    qc"""//$sinkCodeBlocks"""
  }

  private def generateSinkCode(lambdaVal: ValName,
                               propsVal: ValName,
                               sink: StreamSink,
                               lambdaInfo: GeneratedLambdaInfo): CodeBlock = {
    sink.sink match {
      case ddb: DynamoDbTableSink[_] =>
        this.generateDynamoDbTableSinkCode(lambdaVal, propsVal, sink, ddb, lambdaInfo)
    }
  }

  private def generateSourcesCode(applicationInstance: ApplicationInstance,
                                  lambdaVal: ValName,
                                  scopeVal: ValName,
                                  propsVal: ValName,
                                  lambdaInfo: GeneratedLambdaInfo): CodeBlock = {
    val sources = applicationInstance.config.dataSources
    val sourceCodeBlocks = sources.map(source =>
      this.generateSourceCode(lambdaVal, scopeVal, propsVal, source, lambdaInfo)
    )

    qc"""//$sourceCodeBlocks"""
  }

  private def generateSourceCode(lambdaVal: ValName,
                                 scopeVal: ValName,
                                 propsVal: ValName,
                                 source: StreamDataSource,
                                 lambdaInfo: GeneratedLambdaInfo): CodeBlock = {
    source.source match {
      case ddb: DynamoDbStreamSource[_] =>
        this.generateDynamoDbStreamSourceCode(lambdaVal, scopeVal, propsVal, source, ddb, lambdaInfo)
    }
  }

  private def generateDynamoDbTableSinkCode(lambdaVal: ValName,
                                            propsVal: ValName,
                                            sink: StreamSink,
                                            ddb: DynamoDbTableSink[_],
                                            lambdaInfo: GeneratedLambdaInfo): CodeBlock = {
    val outputTableVal = ValName(toValidIdentifier(s"${lambdaVal}_${ddb.tableName}_sink"))

    qc"""
        |const $outputTableVal = $propsVal.${lambdaInfo.sinkConstructs(sink.sink.sinkId).propsField};
        |$outputTableVal.grantReadWriteData($lambdaVal);
        |"""
  }

  private def generateDynamoDbStreamSourceCode(lambdaVal: ValName,
                                               scopeVal: ValName,
                                               propsVal: ValName,
                                               source: StreamDataSource,
                                               ddb: DynamoDbStreamSource[_],
                                               lambdaInfo: GeneratedLambdaInfo): CodeBlock = {
    val eventSourceName = s"${lambdaInfo.functionName}-EventSource-${source.streamId}"

    val inputTableVal = ValName(toValidIdentifier(s"${lambdaVal}_${ddb.tableName}_sink"))

    qc"""const $inputTableVal = $propsVal.${lambdaInfo.sourceConstructs(source.streamId).propsField};
        |
        |new lambda.EventSourceMapping($scopeVal, $eventSourceName, {
        |  target: $lambdaVal,
        |  eventSourceArn: $inputTableVal.tableStreamArn,
        |  startingPosition: lambda.StartingPosition.LATEST,
        |  batchSize: 10,
        |  enabled: true,
        |  retryAttempts: 3,
        |});
        |
        |$inputTableVal.grantStreamRead($lambdaVal);
        |"""
  }

  private def getLambdaInfo(applicationInstance: ApplicationInstance,
                            handlerPackageName: String,
                            handlerClassName: String): GeneratedLambdaInfo = {
    val appId = applicationInstance.application.applicationId
    val constructMethodName = MethodName(toValidIdentifier(s"construct${appId}LambdaFunction"))
    val propsClassName = ClassName(s"${appId}LambdaProps")
    val componentName = toValidIdentifier(appId)
    val functionName = toValidName(appId)
    val handler = s"$handlerPackageName.$handlerClassName::handleRequest"

    val sourceConstructs = applicationInstance.config.dataSources
      .map(source => source.streamId -> this.getSourceConstruct(source)).toMap

    val sinkConstructs = applicationInstance.config.dataSinks
      .map(sink => sink.sink.sinkId -> this.getSinkConstruct(sink)).toMap

    GeneratedLambdaInfo(
      propsClassName,
      constructMethodName,
      componentName,
      functionName,
      handler,
      sourceConstructs,
      sinkConstructs
    )
  }

  private def getSourceConstruct(source: StreamDataSource): SourceConstructInfo = {
    val propsFieldPrefix = toValidIdentifier(s"${source.streamId}Input")

    source.source match {
      case _: DynamoDbStreamSource[_] =>
        SourceConstructInfo(
          source,
          ValName(propsFieldPrefix + "Table"),
          ClassName("dynamodb.ITable"),
          List("""import dynamodb = require("{cdk}/aws-dynamodb");""")
        )

      case _ =>
        throw new NotImplementedError(s"Data sources of type ${source.source.getTypeName} are not supported.")
    }
  }

  private def getSinkConstruct(sink: StreamSink): SinkConstructInfo = {
    val propsFieldPrefix = toValidIdentifier(s"${sink.streamId}Output")

    sink.sink match {
      case _: DynamoDbTableSink[_] =>
        SinkConstructInfo(
          sink,
          ValName(propsFieldPrefix + "Table"),
          ClassName("dynamodb.ITable"),
          List("""import dynamodb = require("{cdk}/aws-dynamodb");""")
        )

      case _ =>
        throw new NotImplementedError(s"Sinks of type ${sink.sink.getTypeName} are not supported.")
    }
  }
}


/**
 *
 * @param propsClassName      The name of the props class used to pass in parameters.
 * @param constructMethodName The name of the method that constructs the lambda function.
 * @param constructId         The ID of the CDK construct created for the function.
 * @param functionName        The name of the lambda function.
 * @param handler             The lambda handler value.
 * @param sourceConstructs    A map of stream IDs to the constructs that represent the data source.
 * @param sinkConstructs      A map of sink IDs to the constructs that represent the data sink target.
 */
case class GeneratedLambdaInfo(propsClassName: ClassName,
                               constructMethodName: MethodName,
                               constructId: String,
                               functionName: String,
                               handler: String,
                               sourceConstructs: Map[String, SourceConstructInfo],
                               sinkConstructs: Map[String, SinkConstructInfo])


case class SourceConstructInfo(source: StreamDataSource,
                               propsField: ValName,
                               constructType: ClassName,
                               importStatements: List[String])


case class SinkConstructInfo(sink: StreamSink,
                             propsField: ValName,
                             constructType: ClassName,
                             importStatements: List[String])


case class GeneratedLambdaConstruct(constructClassName: String,
                                    propsClassName: String)
