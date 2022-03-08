package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.ApplicationInstance
import com.amazon.milan.compiler.scala.event.{EventHandlerGeneratorPlugin, GeneratorOutputInfo}
import com.amazon.milan.compiler.scala.{TypeLifter, toValidIdentifier}
import com.amazon.milan.tools.{ApplicationInstanceCompiler, CompilerOutputs, InstanceParameters}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}


class AwsServerlessCompiler extends ApplicationInstanceCompiler {
  private val logger = Logger(LoggerFactory.getLogger("AwsServerlessCompiler"))

  override def compile(applicationInstance: ApplicationInstance,
                       params: InstanceParameters,
                       outputs: CompilerOutputs): Unit = {
    val scalaFileBase = outputs.getOutput("scala")
    val cdkFileBase = outputs.getOutput("cdk")
    this.compile(applicationInstance, params, scalaFileBase, cdkFileBase)
  }

  def compile(applicationInstance: ApplicationInstance,
              params: InstanceParameters,
              scalaFileBase: Path,
              cdkFileBase: Path): Unit = {
    val className = params.getValue("class")
    val packageName = params.getValue("package")
    val cdkPackage = params.getValueOrDefault("cdk", "aws-cdk-lib")

    val partition = params.getValueOrDefault("partition", "false").toBoolean

    if (!partition) {
      val scalaFile = scalaFileBase.resolve(s"$className.scala")
      val cdkFile = cdkFileBase.resolve(s"$className.ts")
      this.compilePartition(applicationInstance, packageName, className, scalaFile, cdkFile, cdkPackage, params)
    }
    else {
      val partitions = this.getPartitions(applicationInstance)
      val partitionConstructInfo = partitions.map(partition => {
        val partitionClassName = toValidIdentifier(s"$className${partition.application.applicationId}")
        val scalaFile = scalaFileBase.resolve(s"$partitionClassName.scala")
        val cdkFileName = s"$partitionClassName.ts"
        val cdkFile = cdkFileBase.resolve(cdkFileName)
        val partitionCdkInfo = this.compilePartition(partition, packageName, partitionClassName, scalaFile, cdkFile, cdkPackage, params)
        ApplicationPartitionCdkConstructInfo(partition, cdkFileName.toString, partitionCdkInfo)
      })

      this.compilePartitionedApplication(
        applicationInstance.application.applicationId,
        partitionConstructInfo,
        cdkFileBase,
        cdkPackage
      )
    }
  }

  def compilePartition(partitionInstance: ApplicationInstance,
                       packageName: String,
                       className: String,
                       scalaFile: Path,
                       cdkFile: Path,
                       cdkPackage: String,
                       params: InstanceParameters): GeneratedConstructFile = {
    val handlerGeneratorOutput = this.compilePartitionScalaFile(partitionInstance, packageName, className, scalaFile, params)
    this.compilePartitionCdkFile(partitionInstance, handlerGeneratorOutput, packageName, className, cdkFile, cdkPackage, params)
  }

  private def compilePartitionedApplication(applicationName: String,
                                            partitionConstructInfo: List[ApplicationPartitionCdkConstructInfo],
                                            cdkFileBase: Path,
                                            cdkPackage: String): Unit = {
    val applicationCdkGenerator = new ApplicationCdkGenerator

    val constructFileContents =
      applicationCdkGenerator.generatePartitionedApplicationCreationFile(
        applicationName,
        partitionConstructInfo,
        cdkPackage
      )

    val fileContents = constructFileContents.getBytes(StandardCharsets.UTF_8)
    val cdkFile = cdkFileBase.resolve(toValidIdentifier(s"construct$applicationName") + ".ts")
    Files.write(cdkFile, fileContents)
  }

  private def compilePartitionScalaFile(partitionInstance: ApplicationInstance,
                                        packageName: String,
                                        className: String,
                                        scalaFile: Path,
                                        params: InstanceParameters): GeneratorOutputInfo = {
    val generator = new LambdaHandlerGenerator
    val plugin = EventHandlerGeneratorPlugin.loadAllPlugins(TypeLifter.createDefault(), params)

    this.logger.info(s"Using plugins: ${plugin.describe()}")

    val handlerClassInfo = generator.generateLambdaHandlerClass(partitionInstance, plugin, className)
    val fileContents =
      s"""package $packageName
         |
         |${handlerClassInfo.classDef}
         |""".stripMargin
    val fileBytes = fileContents.getBytes(StandardCharsets.UTF_8)

    this.logger.info(s"Writing file '$scalaFile'")

    Files.createDirectories(scalaFile.getParent)
    Files.write(scalaFile, fileBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    handlerClassInfo.handlerGeneratorOutput
  }

  private def compilePartitionCdkFile(partitionInstance: ApplicationInstance,
                                      handlerGeneratorOutput: GeneratorOutputInfo,
                                      packageName: String,
                                      className: String,
                                      cdkFile: Path,
                                      cdkPackage: String,
                                      params: InstanceParameters): GeneratedConstructFile = {
    val jarPath = params.getValue("code")
    val generator = new LambdaCdkGenerator
    val generatorOutput = generator.generateLambdaDefinition(
      partitionInstance, handlerGeneratorOutput, jarPath, packageName, className, cdkPackage
    )

    this.logger.info(s"Writing file '$cdkFile'")

    Files.createDirectories(cdkFile.getParent)

    val fileBytes = generatorOutput.cdkFileContents.getBytes(StandardCharsets.UTF_8)
    Files.write(cdkFile, fileBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    generatorOutput
  }

  private def getPartitions(instance: ApplicationInstance): List[ApplicationInstance] = {
    val mappingProcessor = new SqsInputOutputMappingProcessor
    ApplicationPartitioner.partitionApplication(instance, mappingProcessor)
  }
}
