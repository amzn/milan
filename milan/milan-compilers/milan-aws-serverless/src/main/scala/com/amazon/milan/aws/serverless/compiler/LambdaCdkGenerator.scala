package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.ApplicationConfiguration.{StreamDataSource, StreamSink}
import com.amazon.milan.application.sinks.{DynamoDbTableSink, SqsDataSink}
import com.amazon.milan.application.sources.{DynamoDbStreamSource, SqsDataSource}
import com.amazon.milan.application.state.MemoryStateStore
import com.amazon.milan.application.{ApplicationInstance, StateStore}
import com.amazon.milan.aws.serverless.application.DynamoDbStateStore
import com.amazon.milan.aws.serverless.runtime.MilanLambdaHandler
import com.amazon.milan.compiler.scala.event.{ClassProperty, ClassPropertySourceType, GeneratedStateStore, GeneratorOutputInfo}
import com.amazon.milan.compiler.scala.{ClassName, CodeBlock, MethodName, ValName, toValidIdentifier, toValidName}


case class GeneratedConstructFile(cdkFileContents: String,
                                  constructMethodName: MethodName,
                                  propsClassName: ClassName,
                                  sourceConstructs: List[SourceConstructInfo],
                                  sinkConstructs: List[SinkConstructInfo],
                                  additionalPropsFields: List[PropsField])


class LambdaCdkGenerator extends CdkHelper {

  protected val typeLifter = new TypeScriptLifter

  import typeLifter._

  /**
   * Generates a typescript file with method that creates a lambda function construct.
   *
   * @param applicationInstance The application instance executing in the generated lambda.
   * @param handlerPackage      The java package containing the lambda handler class.
   * @param cdkPackage          The name of the nodejs CDK package, e.g. "aws-cdk-lib".
   * @return A [[CodeBlock]] containing the contents of the file.
   */
  def generateLambdaDefinition(applicationInstance: ApplicationInstance,
                               handlerGeneratorOutput: GeneratorOutputInfo,
                               jarPath: String,
                               handlerPackage: String,
                               handlerClassName: String,
                               cdkPackage: String): GeneratedConstructFile = {
    val lambdaInfo = this.getLambdaInfo(applicationInstance, handlerGeneratorOutput, handlerPackage, handlerClassName)
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
                               baseClassName: String): GeneratedConstructFile = {
    val additionalPropsFields = List(
      PropsField(ValName("timeout"), ClassName("cdk.Duration"), optional = true, List.empty),
      PropsField(ValName("memorySize"), ClassName("number"), optional = true, List.empty),
      PropsField(ValName("environment"), ClassName("object"), optional = true, List.empty)
    )

    val propsClass = this.generateLambdaPropsClass(lambdaInfo, additionalPropsFields)
    val constructMethod = this.generateLambdaConstructMethod(applicationInstance, lambdaInfo, jarPath, ClassName(baseClassName))

    val allImports = lambdaInfo.sourceConstructs.values.flatMap(_.importStatements).toList ++
      lambdaInfo.sinkConstructs.values.flatMap(_.importStatements).toList ++
      constructMethod.imports

    val imports = this.generateImports(allImports, cdkPackage)

    val fileContents =
      qc"""$imports
          |import lambda = require(${cdkPackage + "/aws-lambda"});
          |
          |$propsClass
          |
          |${constructMethod.code}
          |""".toString

    GeneratedConstructFile(
      fileContents,
      lambdaInfo.constructMethodName,
      lambdaInfo.propsClassName,
      lambdaInfo.sourceConstructs.values.toList,
      lambdaInfo.sinkConstructs.values.toList,
      additionalPropsFields
    )
  }

  /**
   * Generates the definition of the Props interface used to pass dependencies to the lambda construct creation
   * method.
   *
   * @param lambdaInfo Generated lambda function information.
   * @return A code block containing the definition of the Props interface.
   */
  private def generateLambdaPropsClass(lambdaInfo: GeneratedLambdaInfo,
                                       additionalPropsFields: List[PropsField]): CodeBlock = {
    val sourceFields =
      lambdaInfo.sourceConstructs.values.map(info => qc"readonly ${info.propsField}: ${info.constructType};")
    val sourceFieldsSnippet = qc"""//$sourceFields"""

    val sinkFields =
      lambdaInfo.sinkConstructs.values.map(info => qc"readonly ${info.propsField}: ${info.constructType};")
    val sinkFieldsSnippet = qc"""//$sinkFields"""

    val stateFields =
      lambdaInfo.stateStoreConstructs.map(info => qc"readonly ${info.propsField}: ${info.constructType};")
    val stateFieldsSnippet = qc"""//$stateFields"""

    val additionalFields =
      additionalPropsFields.map(field => qc"readonly ${field.fieldName}${code(if (field.optional) "?" else "")}: ${field.fieldType};")
    val additionalFieldsSnippet = qc"""//$additionalFields"""

    qc"""export interface ${lambdaInfo.propsClassName} {
        |  ${sourceFieldsSnippet.indentTail(1)}
        |  ${sinkFieldsSnippet.indentTail(1)}
        |  ${stateFieldsSnippet.indentTail(1)}
        |  ${additionalFieldsSnippet.indentTail(1)}
        |}
        |"""
  }

  /**
   * Generates a method that creates a lambda function CDK construct for an application instance.
   *
   * @param applicationInstance An application instance.
   * @param lambdaInfo          Generated lambda function information.
   * @param jarPath             The path to the jar containing the lambda handler.
   * @param baseClassName       The name of the base class to use for the lambda function construct, usually
   *                            "lambda.Function".
   * @return A code block defining a method that creates and returns the CDK construct for the lambda for the
   *         application.
   */
  private def generateLambdaConstructMethod(applicationInstance: ApplicationInstance,
                                            lambdaInfo: GeneratedLambdaInfo,
                                            jarPath: String,
                                            baseClassName: ClassName): CodeWithImports = {
    val scopeVal = ValName("scope")
    val lambdaVal = ValName("lambdaFunction")
    val propsVal = ValName("props")

    val triggerCode = this.generateSourcesCode(applicationInstance, lambdaVal, scopeVal, propsVal, lambdaInfo)
    val sinkCode = this.generateSinkCode(applicationInstance, lambdaVal, propsVal, lambdaInfo)
    val stateStoreCode = this.generateStateStoreCode(scopeVal, lambdaVal, lambdaInfo)

    // Get environment variables that arise due to the need to check for event source ARN prefixes
    // in the lambda handler.
    val sourceConstructArnPrefixEnvironment = lambdaInfo.sourceConstructs.map { case (streamId, construct) =>
      val envVar = MilanLambdaHandler.getEventSourceArnPrefixEnvironmentVariable(streamId)
      val arn = this.getEventSourceArnSnippet(qc"""$propsVal.${construct.propsField}""", construct)

      // We have to make sure the snippets evaluate to strings.
      envVar -> qc"`$${$arn}`"
    }

    // Get the environment variables that arise due to the properties used by the generated handler code.
    val propertyEnvironment =
      this.generatePropertyEnvironmentValues(
        propsVal,
        lambdaInfo,
        stateStoreCode.stateStoreConstructs
      )

    // Combine them to get the full environment for the lambda.
    val fullEnvironment =
      (sourceConstructArnPrefixEnvironment ++ propertyEnvironment).map {
        case (key, value) => qc"$key: $value,"
      }

    val environmentCode = qc"//$fullEnvironment"

    val functionCode =
      qc"""export function ${lambdaInfo.constructMethodName}($scopeVal: Construct, $propsVal: ${lambdaInfo.propsClassName}) {
          |  ${stateStoreCode.beforeLambdaCode.map(_.code).getOrElse(qc"").indentTail(1)}
          |
          |  const $lambdaVal = new $baseClassName($scopeVal, ${lambdaInfo.constructId}, {
          |    functionName: ${lambdaInfo.functionName},
          |    handler: ${lambdaInfo.handler},
          |    runtime: lambda.Runtime.JAVA_8,
          |    code: lambda.Code.fromAsset($jarPath),
          |    environment: {
          |      ${environmentCode.indentTail(3)}
          |      ${code("...")}$propsVal.environment
          |    },
          |    timeout: props.timeout,
          |    memorySize: props.memorySize,
          |  });
          |
          |  ${triggerCode.indentTail(1)}
          |
          |  ${sinkCode.indentTail(1)}
          |
          |  ${stateStoreCode.afterLambdaCode.map(_.code).getOrElse(qc"").indentTail(1)}
          |
          |  return $lambdaVal;
          |}
          |"""

    val imports = (stateStoreCode.afterLambdaCode.map(_.imports).getOrElse(List.empty) ++
      stateStoreCode.beforeLambdaCode.map(_.imports).getOrElse(List.empty))
      .distinct

    CodeWithImports(functionCode, imports)
  }

  /**
   * Gets lambda environment values for the class properties that are used by the generated handler code.
   *
   * @param propsVal   A [[ValName]] that refers to the properties passed into the function that creates the
   *                   lambda construct.
   * @param lambdaInfo Information about the generated lambda handler.
   * @return A map of environment variable names to values.
   */
  private def generatePropertyEnvironmentValues(propsVal: ValName,
                                                lambdaInfo: GeneratedLambdaInfo,
                                                inlineStateStoreConstructs: List[InlineStateStoreConstructInfo]): Map[String, CodeBlock] = {
    def getStateStoreConstructReference(streamId: String, stateId: String): ValName = {
      lambdaInfo.getStateStoreConstruct(streamId, stateId) match {
        case Some(construct) =>
          construct.propsField

        case None =>
          inlineStateStoreConstructs.find(info => info.streamId == streamId && info.stateId == stateId).get.constructVal
      }
    }

    def getConstructReference(prop: ClassProperty): CodeBlock = {
      prop.propertySource.sourceType match {
        case ClassPropertySourceType.StreamSink =>
          val sinkId = prop.propertySource.sourceId.split(':').last
          qc"$propsVal.${lambdaInfo.sinkConstructs(sinkId).propsField}"

        case ClassPropertySourceType.StreamSource =>
          qc"$propsVal.${lambdaInfo.sourceConstructs(prop.propertySource.sourceId).propsField}"

        case ClassPropertySourceType.StateStore =>
          val sourceIdParts = prop.propertySource.sourceId.split(':')
          val streamId = sourceIdParts(0)
          val stateId = sourceIdParts(1)
          getStateStoreConstructReference(streamId, stateId)
      }
    }

    def getPropertyValueSnippet(prop: ClassProperty): CodeBlock = {
      val constructReference = getConstructReference(prop)

      // We wrap the property values in ticks so they evaluate to strings.
      // Many of the properties we reference are string | undefined, which isn't allowed in the environment
      // property of lambda functions.
      prop.propertySource.sourceProperty match {
        case "TableName" =>
          qc"`$${$constructReference.tableName}`"

        case "QueueUrl" =>
          qc"`$${$constructReference.queueUrl}`"

        case other =>
          throw new IllegalArgumentException(s"Unsupported source property '$other'.")
      }
    }

    lambdaInfo.handlerGeneratorOutput.properties.map(prop => prop.propertyName -> getPropertyValueSnippet(prop)).toMap
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
    val actualSource = toConcreteDataSource(construct.source.source)

    actualSource match {
      case _: DynamoDbStreamSource[_] =>
        qc"""$constructReference.tableStreamArn"""

      case _: SqsDataSource[_] =>
        qc"""$constructReference.queueArn"""
    }
  }

  private def generateStateStoreCode(scopeVal: ValName,
                                     lambdaVal: ValName,
                                     lambdaInfo: GeneratedLambdaInfo): GeneratedStateStoreCode = {
    val stateStores = lambdaInfo.handlerGeneratorOutput.stateStores
    val stateStoreCode = stateStores
      .map(store => this.generateStateStoreCode(store, scopeVal, lambdaVal))
      .filter(_.isDefined)
      .map(_.get)

    val beforeLambdaCode = qc"//${stateStoreCode.map(_.beforeLambdaCode).filter(_.isDefined).map(_.get.code)}"
    val beforeLambdaImports = stateStoreCode.flatMap(_.beforeLambdaCode).flatMap(_.imports)

    val afterLambdaCode = qc"//${stateStoreCode.map(_.afterLambdaCode).filter(_.isDefined).map(_.get).map(_.code)}"
    val afterLambdaImports = stateStoreCode.flatMap(_.afterLambdaCode).flatMap(_.imports)

    val allConstructs = stateStoreCode.flatMap(_.stateStoreConstructs)

    GeneratedStateStoreCode(
      Some(CodeWithImports(beforeLambdaCode, beforeLambdaImports)),
      Some(CodeWithImports(afterLambdaCode, afterLambdaImports)),
      allConstructs
    )
  }

  private def generateStateStoreCode(store: GeneratedStateStore,
                                     scopeVal: ValName,
                                     lambdaVal: ValName): Option[GeneratedStateStoreCode] = {
    store.stateStore match {
      case _: DynamoDbStateStore =>
        val tableVal = ValName(toValidIdentifier(s"${store.streamId}_${store.stateId}_stateTable"))
        val tableName = s"${store.streamId}-${store.stateId}-State"

        val beforeLambdaCode =
          qc"""const $tableVal = new dynamodb.Table($scopeVal, $tableName, {
              |  tableName: $tableName,
              |  partitionKey: {name: "key", type: dynamodb.AttributeType.STRING},
              |  billingMode: dynamodb.BillingMode.PAY_PER_REQUEST
              |});
              |"""
        val beforeLambdaImports = List("""import dynamodb = require("{cdk}/aws-dynamodb");""")

        val afterLambdaCode = this.generateGrantDynamoDbReadWrite(lambdaVal, tableVal)

        val constructs = List(InlineStateStoreConstructInfo(store.streamId, store.stateId, tableVal))

        Some(GeneratedStateStoreCode(
          Some(CodeWithImports(beforeLambdaCode, beforeLambdaImports)),
          Some(CodeWithImports(afterLambdaCode, List.empty)),
          constructs
        ))

      case _: MemoryStateStore =>
        None
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
        val propsField = lambdaInfo.sinkConstructs(ddb.sinkId).propsField
        val tableReference = qc"$propsVal.$propsField"
        this.generateGrantDynamoDbReadWrite(lambdaVal, tableReference)

      case sqs: SqsDataSink[_] =>
        this.generateSqsSinkCode(lambdaVal, propsVal, sqs, lambdaInfo)
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
    val actualSource = toConcreteDataSource(source.source)

    actualSource match {
      case _: DynamoDbStreamSource[_] =>
        this.generateDynamoDbStreamSourceCode(lambdaVal, scopeVal, propsVal, source, lambdaInfo)

      case _: SqsDataSource[_] =>
        this.generateSqsSourceCode(lambdaVal, propsVal, source, lambdaInfo)
    }
  }

  private def generateSqsSinkCode(lambdaVal: ValName,
                                  propsVal: ValName,
                                  sqs: SqsDataSink[_],
                                  lambdaInfo: GeneratedLambdaInfo): CodeBlock = {
    val outputQueueVal = ValName(toValidIdentifier(s"${lambdaVal}_${sqs.sinkId}_sink"))

    qc"""
        |const $outputQueueVal = $propsVal.${lambdaInfo.sinkConstructs(sqs.sinkId).propsField};
        |$outputQueueVal.grantSendMessages($lambdaVal);
        |"""
  }

  private def generateDynamoDbStreamSourceCode(lambdaVal: ValName,
                                               scopeVal: ValName,
                                               propsVal: ValName,
                                               source: StreamDataSource,
                                               lambdaInfo: GeneratedLambdaInfo): CodeBlock = {
    val eventSourceName = s"${lambdaInfo.functionName}-EventSource-${source.streamId}"

    val inputTableVal = ValName(toValidIdentifier(s"${lambdaVal}_${source.streamId}_source"))

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

  private def generateSqsSourceCode(lambdaVal: ValName,
                                    propsVal: ValName,
                                    source: StreamDataSource,
                                    lambdaInfo: GeneratedLambdaInfo): CodeBlock = {
    val inputQueueVal = ValName(toValidIdentifier(s"${lambdaVal}_${source.streamId}_source"))

    qc"""const $inputQueueVal = $propsVal.${lambdaInfo.sourceConstructs(source.streamId).propsField};
        |
        |$lambdaVal.addEventSource(new sources.SqsEventSource($inputQueueVal, {
        |  batchSize: 10,
        |  enabled: true
        |}));
        |"""
  }

  private def getLambdaInfo(applicationInstance: ApplicationInstance,
                            handlerGeneratorOutput: GeneratorOutputInfo,
                            handlerPackageName: String,
                            handlerClassName: String): GeneratedLambdaInfo = {
    val appId = applicationInstance.application.applicationId
    val constructMethodName = MethodName(toValidIdentifier(s"construct${appId}LambdaFunction"))
    val propsClassName = ClassName(toValidIdentifier(s"${appId}LambdaProps"))
    val componentName = toValidIdentifier(appId)
    val functionName = toValidName(appId)
    val handler = s"$handlerPackageName.$handlerClassName::handleRequest"

    val sourceConstructs = applicationInstance.config.dataSources
      .map(source => source.streamId -> this.getSourceConstruct(source)).toMap

    val sinkConstructs = applicationInstance.config.dataSinks
      .map(sink => sink.sink.sinkId -> this.getSinkConstruct(sink)).toMap

    val stateStoreConstructs = handlerGeneratorOutput.stateStores
      .map(store => this.getStateStoreConstruct(store))
      .filter(_.isDefined)
      .map(_.get)

    GeneratedLambdaInfo(
      handlerGeneratorOutput,
      propsClassName,
      constructMethodName,
      componentName,
      functionName,
      handler,
      sourceConstructs,
      sinkConstructs,
      stateStoreConstructs
    )
  }

  private def getStateStoreConstruct(store: GeneratedStateStore): Option[ExternalStateStoreConstructInfo] = {
    None
  }

  private def getSourceConstruct(source: StreamDataSource): SourceConstructInfo = {
    val propsFieldPrefix = toValidIdentifier(s"${source.streamId}Input")
    val actualSource = toConcreteDataSource(source.source)

    actualSource match {
      case _: DynamoDbStreamSource[_] =>
        SourceConstructInfo(
          source,
          ValName(propsFieldPrefix + "Table"),
          ClassName("dynamodb.ITable"),
          List("""import dynamodb = require("{cdk}/aws-dynamodb");""")
        )

      case _: SqsDataSource[_] =>
        SourceConstructInfo(
          source,
          ValName(propsFieldPrefix + "Queue"),
          ClassName("sqs.IQueue"),
          List("""import sqs = require("{cdk}/aws-sqs");""", """import sources = require("{cdk}/aws-lambda-event-sources");""")
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

      case _: SqsDataSink[_] =>
        SinkConstructInfo(
          sink,
          ValName(propsFieldPrefix + "Queue"),
          ClassName("sqs.IQueue"),
          List("""import sqs = require("{cdk}/aws-sqs");""")
        )

      case _ =>
        throw new NotImplementedError(s"Sinks of type ${sink.sink.getTypeName} are not supported.")
    }
  }

  private def generateGrantDynamoDbReadWrite(lambdaVal: ValName,
                                             tableReference: CodeBlock): CodeBlock = {
    qc"$tableReference.grantReadWriteData($lambdaVal);"
  }
}


/**
 * @param handlerGeneratorOutput Information returned by the handler code generator.
 * @param propsClassName         The name of the props class used to pass in parameters.
 * @param constructMethodName    The name of the method that constructs the lambda function.
 * @param constructId            The ID of the CDK construct created for the function.
 * @param functionName           The name of the lambda function.
 * @param handler                The lambda handler value.
 * @param sourceConstructs       A map of stream IDs to the constructs that represent the data source.
 * @param sinkConstructs         A map of sink IDs to the constructs that represent the data sink target.
 * @param stateStoreConstructs   The constructs that represent state store locations.
 */
case class GeneratedLambdaInfo(handlerGeneratorOutput: GeneratorOutputInfo,
                               propsClassName: ClassName,
                               constructMethodName: MethodName,
                               constructId: String,
                               functionName: String,
                               handler: String,
                               sourceConstructs: Map[String, SourceConstructInfo],
                               sinkConstructs: Map[String, SinkConstructInfo],
                               stateStoreConstructs: List[ExternalStateStoreConstructInfo]) {
  def getStateStoreConstruct(streamId: String, stateId: String): Option[ExternalStateStoreConstructInfo] = {
    stateStoreConstructs.find(s => s.streamId == streamId && s.stateId == stateId)
  }
}


case class PropsField(fieldName: ValName,
                      fieldType: ClassName,
                      optional: Boolean,
                      importStatements: List[String])


case class SourceConstructInfo(source: StreamDataSource,
                               propsField: ValName,
                               constructType: ClassName,
                               importStatements: List[String])


case class SinkConstructInfo(sink: StreamSink,
                             propsField: ValName,
                             constructType: ClassName,
                             importStatements: List[String])

case class ExternalStateStoreConstructInfo(streamId: String,
                                           stateId: String,
                                           stateStore: StateStore,
                                           propsField: ValName,
                                           constructType: ClassName,
                                           importStatements: List[String])

case class InlineStateStoreConstructInfo(streamId: String,
                                         stateId: String,
                                         constructVal: ValName)

case class GeneratedLambdaConstruct(constructClassName: String,
                                    propsClassName: String)


case class CodeWithImports(code: CodeBlock, imports: List[String])


case class GeneratedStateStoreCode(beforeLambdaCode: Option[CodeWithImports],
                                   afterLambdaCode: Option[CodeWithImports],
                                   stateStoreConstructs: List[InlineStateStoreConstructInfo])
