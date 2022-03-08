package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.sinks.SqsDataSink
import com.amazon.milan.application.{ApplicationInstance, DataSink}
import com.amazon.milan.aws.serverless.application.StreamPointerDataSource
import com.amazon.milan.compiler.scala.{ClassName, CodeBlock, MethodName, ValName, toValidIdentifier}
import org.apache.commons.io.FilenameUtils


/**
 * Contains information about the method that creates the CDK construct for an application partition.
 *
 * @param partition           The application instance that defines the application partition.
 * @param cdkFileName         The name of the file containing the method that creates the construct.
 * @param lambdaConstructFile The output of the lambda handler generator.
 */
case class ApplicationPartitionCdkConstructInfo(partition: ApplicationInstance,
                                                cdkFileName: String,
                                                lambdaConstructFile: GeneratedConstructFile)


/**
 * Generates a typescript file that creates all of the CDK constructs for an application.
 * Before using this class, the partitions of the application must be generated using [[LambdaCdkGenerator]].
 */
class ApplicationCdkGenerator extends CdkHelper {
  protected val typeLifter = new TypeScriptLifter

  import typeLifter._

  /**
   * Generates the contents of a typescript file, which contains a method to construct a complete partitioned
   * application.
   *
   * @param applicationName The name of the application.
   * @param partitions      The application partitions, which have already been generated.
   * @param cdkPackage      The cdk package (e.g. "aws-sdk-lib")
   * @return A string containing the generated file contents.
   */
  def generatePartitionedApplicationCreationFile(applicationName: String,
                                                 partitions: List[ApplicationPartitionCdkConstructInfo],
                                                 cdkPackage: String): String = {
    // This method needs to do two things.
    // It needs to call the construct methods for all of the partitions.
    // But first, it needs to create any infrastructure required to connect the partitions together.
    // Any inputs and outputs of the non-partitioned application must be supplied by the caller of the generated method,
    // but any inputs and outputs of the partitions, which are a result of the partitioning operation, need to be
    // created by the method we are generating now.

    val partitioningInputs = partitions.flatMap(partition =>
      this.getPartitioningInputs(partition).map(sourceConstruct => (partition.partition, sourceConstruct))
    )

    val connections = partitioningInputs.map { case (destinationPartition, destinationConstruct) =>
      val pointer = destinationConstruct.source.source.asInstanceOf[StreamPointerDataSource[_]]
      val sourcePartition = pointer.sourceInstance

      // Find the source construct for the input stream to the destination partition.
      // We do this by looking at the source partition and finding the sink that matches the sink from the pointer.
      val sourceConstructFile = partitions.find(_.partition == sourcePartition).get.lambdaConstructFile
      val sourceConstruct = sourceConstructFile.sinkConstructs.find(_.sink.sink == pointer.sourceSink).get

      val connectionConstructVal = ValName(toValidIdentifier(s"${sourceConstruct.sink.streamId}_${destinationConstruct.source.streamId}_connection"))

      PartitionConnection(
        sourcePartition,
        sourceConstruct,
        destinationPartition,
        destinationConstruct,
        pointer.sourceSink,
        connectionConstructVal
      )
    }

    // Gather all of the inputs (sources) and outputs (sinks) from the application.
    // These are the ones we need to get from the caller of the generated code.
    val connectionOutputs = connections.map(_.sourceConstruct).toSet
    val connectionInputs = connections.map(_.destinationConstruct).toSet

    val applicationInputs = partitions.flatMap(_.lambdaConstructFile.sourceConstructs.filter(c => !connectionInputs.contains(c)))
    val applicationOutputs = partitions.flatMap(_.lambdaConstructFile.sinkConstructs.filter(c => !connectionOutputs.contains(c)))

    val additionalProps = this.getAdditionalApplicationProps(partitions)

    val propsClassName = ClassName(toValidIdentifier(s"${applicationName}Props"))
    val propsClassDef = this.generateApplicationPropsClass(propsClassName, applicationInputs, applicationOutputs, additionalProps)

    val constructImports = applicationInputs.flatMap(_.importStatements) ++ applicationOutputs.flatMap(_.importStatements)
    val partitionMethodImports = this.getPartitionMethodImports(partitions)

    val applicationConstructMethodInfo = this.generateApplicationConstructMethod(
      applicationName,
      propsClassName,
      connections,
      partitions
    )

    val propsFieldImports = partitions.flatMap(_.lambdaConstructFile.additionalPropsFields.flatMap(_.importStatements)).distinct

    val allImports = constructImports ++ partitionMethodImports ++ applicationConstructMethodInfo.importStatements ++ propsFieldImports

    val imports = this.generateImports(allImports, cdkPackage)

    val fileContents =
      qc"""$imports
          |
          |$propsClassDef
          |
          |${applicationConstructMethodInfo.methodDefinition}
          |""".toString

    fileContents
  }

  /**
   * Generates the import statements that import the construction methods for the partitions of an application.
   *
   * @param partitions The application partitions.
   * @return A [[CodeBlock]] containing the import statements.
   */
  private def getPartitionMethodImports(partitions: List[ApplicationPartitionCdkConstructInfo]): List[String] = {
    partitions.map(partition => {
      val fileNameWithoutExtension = FilenameUtils.removeExtension(partition.cdkFileName)
      s"""import { ${partition.lambdaConstructFile.constructMethodName} } from "./$fileNameWithoutExtension";"""
    })
  }

  /**
   * Generates a method that constructs a partitioned application.
   *
   * @param applicationName The name of the application.
   * @param propsClassName  The name of the Props class that contains application dependencies.
   * @param connections     Connections between partitions.
   * @param partitions      The application partitions.
   * @return A [[CodeBlock]] that defines the method that constructs the application.
   */
  private def generateApplicationConstructMethod(applicationName: String,
                                                 propsClassName: ClassName,
                                                 connections: List[PartitionConnection],
                                                 partitions: List[ApplicationPartitionCdkConstructInfo]): ApplicationConstructMethodInfo = {
    val applicationConstructMethodName = MethodName(s"construct$applicationName")
    val scopeVal = ValName("scope")
    val propsVal = ValName("props")

    val connectionConstructs = this.generateConnectionConstructs(scopeVal, connections)
    val connectionsCode = qc"//${connectionConstructs.map(_.creationSnippet)}"
    val connectionImports = connectionConstructs.map(_.importStatement)

    val additionalProps = this.getAdditionalApplicationProps(partitions)

    val partitionConstructSnippets =
      partitions.map(partition => this.generatePartitionConstructCall(partition, connections, additionalProps, scopeVal, propsVal))

    val partitionConstructCode = qc"//$partitionConstructSnippets"

    val methodDef =
      qc"""export function $applicationConstructMethodName($scopeVal: Construct, $propsVal: $propsClassName) {
          |  ${connectionsCode.indentTail(1)}
          |
          |  ${partitionConstructCode.indentTail(1)}
          |}
          |"""

    ApplicationConstructMethodInfo(methodDef, connectionImports)
  }

  /**
   * Generates code that creates the props instance for a partition creation function call and calls the creation
   * function.
   *
   * @param partition                  The partition to create.
   * @param additionalApplicationProps Additional fields of the Props class that are used by the partitions.
   * @param connections                Partition connections for the application.
   * @param scopeVal                   The value that refers to the scope of the CDK constructs.
   * @param applicationPropsVal        The value that refers to the application Props instance passed by the caller.
   * @return A [[CodeBlock]] that creates the partition.
   */
  private def generatePartitionConstructCall(partition: ApplicationPartitionCdkConstructInfo,
                                             connections: List[PartitionConnection],
                                             additionalApplicationProps: AdditionalApplicationPropsFields,
                                             scopeVal: ValName,
                                             applicationPropsVal: ValName): CodeBlock = {
    val propsInstance = this.generatePartitionProps(partition, connections, additionalApplicationProps, applicationPropsVal)
    qc"${partition.lambdaConstructFile.constructMethodName}($scopeVal, $propsInstance);"
  }

  /**
   * Generates code that creates an instance of the props for a partition creation function call.
   *
   * @param partition                  The partition being created.
   * @param connections                Partition connections for the application.
   * @param additionalApplicationProps Additional fields of the Props class that are used by the partitions.
   * @param applicationPropsVal        A [[ValName]] that references the application props object.
   * @return A [[CodeBlock]] that creates an instance of the props for partition creation.
   */
  private def generatePartitionProps(partition: ApplicationPartitionCdkConstructInfo,
                                     connections: List[PartitionConnection],
                                     additionalApplicationProps: AdditionalApplicationPropsFields,
                                     applicationPropsVal: ValName): CodeBlock = {
    def getSourceConstructPropsFieldSource(construct: SourceConstructInfo): CodeBlock = {
      construct.source.source match {
        case pointer: StreamPointerDataSource[_] =>
          // We're looking for the source for a props field that is a data source, so check in the sink constructs in the
          // connections.
          val connection = connections.find(_.sourceConstruct.sink.sink == pointer.sourceSink).get
          qc"${connection.constructVal}"

        case _ =>
          qc"$applicationPropsVal.${construct.propsField}"
      }
    }

    def getSinkConstructPropsFieldSource(construct: SinkConstructInfo): CodeBlock = {
      connections.find(_.sourceConstruct == construct) match {
        case Some(connection) =>
          qc"${connection.constructVal}"

        case None =>
          qc"$applicationPropsVal.${construct.propsField}"
      }
    }

    def getAdditionalPropsFieldSource(field: PropsField): Option[CodeBlock] = {
      val applicationField = additionalApplicationProps.partitionFields.find(appField => appField.targetPropsField == field)

      applicationField.map(appField =>
        appField.defaultFieldName match {
          case Some(default) =>
            // We have a default in the application props so use that as a fallback.
            val defaultReference = qc"$applicationPropsVal.$default"
            qc"$defaultReference ? $defaultReference : $applicationPropsVal.${appField.fieldName}"

          case None =>
            // No default so just copy the value from the application props.
            qc"$applicationPropsVal.${appField.fieldName}"
        }
      )
    }

    val propsFields =
      partition.lambdaConstructFile.sinkConstructs.map(sinkConstruct => {
        val valueSnippet = getSinkConstructPropsFieldSource(sinkConstruct)
        qc"${sinkConstruct.propsField}: $valueSnippet,"
      }) ++
        partition.lambdaConstructFile.sourceConstructs.map(sourceConstruct => {
          val valueSnippet = getSourceConstructPropsFieldSource(sourceConstruct)
          qc"${sourceConstruct.propsField}: $valueSnippet,"
        })

    val propsFieldsSnippet = qc"//$propsFields"

    val additionalFields =
      partition.lambdaConstructFile.additionalPropsFields.map(field =>
        getAdditionalPropsFieldSource(field).map(valueSnippet =>
          qc"${field.fieldName}: $valueSnippet,"
        )
      )
        .filter(_.isDefined)
        .map(_.get)

    val additionalFieldsSnippet = qc"//$additionalFields"

    qc"""{
        |  ${propsFieldsSnippet.indentTail(1)}
        |  ${additionalFieldsSnippet.indentTail(1)}
        |}
        |"""
  }

  /**
   * Generates the Props class for passing in dependencies (inputs, outputs, etc) for an application.
   *
   * @param propsClassName The name of the class to generate.
   * @param inputs         Application inputs, which will have been returned when generating the partitions.
   *                       These will become fields in the generated class.
   * @param outputs        Application outputs, which will have been returned when generating the partitions.
   *                       These will become fields in the generated class.
   * @return A [[CodeBlock]] containing the definition of the Props class.
   */
  private def generateApplicationPropsClass(propsClassName: ClassName,
                                            inputs: List[SourceConstructInfo],
                                            outputs: List[SinkConstructInfo],
                                            additionalProps: AdditionalApplicationPropsFields): CodeBlock = {
    val inputFields = inputs.map(info => qc"readonly ${info.propsField}: ${info.constructType};")
    val inputsSnippet = qc"//$inputFields"

    val outputFields = outputs.map(info => qc"readonly ${info.propsField}: ${info.constructType};")
    val outputsSnippet = qc"//$outputFields"

    val defaultFields = additionalProps.defaultFields.map(info => qc"readonly ${info.fieldName}${code(if (info.optional) "?" else "")}: ${info.fieldType};")
    val defaultFieldsSnippet = qc"//$defaultFields"

    val partitionFields = additionalProps.partitionFields.map(info => qc"readonly ${info.fieldName}${code(if (info.optional) "?" else "")}: ${info.fieldType};")
    val partitionFieldsSnippet = qc"//$partitionFields"

    qc"""export interface $propsClassName {
        |  ${inputsSnippet.indentTail(1)}
        |  ${outputsSnippet.indentTail(1)}
        |  ${defaultFieldsSnippet.indentTail(1)}
        |  ${partitionFieldsSnippet.indentTail(1)}
        |}
        |"""
  }

  /**
   * Gets the [[SourceConstructInfo]] for all inputs to a partition that are connected to another partition.
   */
  private def getPartitioningInputs(partition: ApplicationPartitionCdkConstructInfo): List[SourceConstructInfo] = {
    partition.lambdaConstructFile.sourceConstructs.filter(_.source.source.isInstanceOf[StreamPointerDataSource[_]])
  }

  private def generateConnectionConstructs(scopeVal: ValName,
                                           connections: List[PartitionConnection]): List[ConnectionConstruct] = {
    connections.map(this.generateConnectionConstruct(scopeVal))
  }

  private def generateConnectionConstruct(scopeVal: ValName)(connection: PartitionConnection): ConnectionConstruct = {
    val constructId = connection.constructVal.toString

    connection.sourceConstruct.sink.sink match {
      case _: SqsDataSink[_] =>
        val creationSnippet =
          qc"""
              |const ${connection.constructVal} = new sqs.Queue($scopeVal, $constructId, {
              |  queueName: $constructId,
              |  visibilityTimeout: cdk.Duration.minutes(15),
              |  retentionPeriod: cdk.Duration.days(10),
              |});
              |"""

        val importStatement = """import sqs = require("{cdk}/aws-sqs");"""
        ConnectionConstruct(importStatement, creationSnippet)
    }
  }

  private def getAdditionalApplicationProps(partitions: List[ApplicationPartitionCdkConstructInfo]): AdditionalApplicationPropsFields = {
    val fieldNames = partitions.flatMap(_.lambdaConstructFile.additionalPropsFields)
      .groupBy(_.fieldName.value)
      .toList
      .map { case (name, fields) => (name, fields) }

    val defaultFields =
      fieldNames
        .filter { case (_, fields) => fields.length > 1 }
        .map { case (name, fields) =>
          ApplicationPropsDefaultField(ValName(s"default_$name"), fields.head.fieldType, optional = true, name)
        }

    val partitionFields =
      partitions.flatMap(partition =>
        partition.lambdaConstructFile.additionalPropsFields.map(field => {
          val defaultField = defaultFields.find(_.defaultOf == field.fieldName.value).map(_.fieldName)

          ApplicationPropsPartitionField(
            ValName(toValidIdentifier(s"${partition.partition.application.applicationId}_${field.fieldName}")),
            field.fieldType,
            field.optional,
            defaultField,
            partition,
            field)
        })
      )

    AdditionalApplicationPropsFields(defaultFields, partitionFields)
  }
}


case class PartitionConnection(sourcePartition: ApplicationInstance,
                               sourceConstruct: SinkConstructInfo,
                               destinationPartition: ApplicationInstance,
                               destinationConstruct: SourceConstructInfo,
                               sink: DataSink[_],
                               constructVal: ValName)


case class ConnectionConstruct(importStatement: String, creationSnippet: CodeBlock)


case class ApplicationConstructMethodInfo(methodDefinition: CodeBlock, importStatements: List[String])


case class AdditionalApplicationPropsFields(defaultFields: List[ApplicationPropsDefaultField],
                                            partitionFields: List[ApplicationPropsPartitionField])


case class ApplicationPropsDefaultField(fieldName: ValName,
                                        fieldType: ClassName,
                                        optional: Boolean,
                                        defaultOf: String)

case class ApplicationPropsPartitionField(fieldName: ValName,
                                          fieldType: ClassName,
                                          optional: Boolean,
                                          defaultFieldName: Option[ValName],
                                          targetPartition: ApplicationPartitionCdkConstructInfo,
                                          targetPropsField: PropsField)
