package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.ApplicationInstance
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
    val className = params.getValue("class")
    val packageName = params.getValue("package")

    if (!params.getValueOrDefault("partition", "false").toBoolean) {
      val scalaFile = outputs.getOutput("scala").resolve(s"$className.scala")
      val cdkFile = outputs.getOutput("cdk").resolve(s"$className.ts")
      this.compilePartition(applicationInstance, packageName, className, scalaFile, cdkFile, params)
    }
  }

  def compilePartition(partitionInstance: ApplicationInstance,
                       packageName: String,
                       className: String,
                       scalaFile: Path,
                       cdkFile: Path,
                       params: InstanceParameters): Unit = {
    this.compilePartitionScalaFile(partitionInstance, packageName, className, scalaFile)
    this.compilePartitionCdkFile(partitionInstance, packageName, className, cdkFile, params)
  }

  private def compilePartitionScalaFile(partitionInstance: ApplicationInstance,
                                        packageName: String,
                                        className: String,
                                        scalaFile: Path): Unit = {
    val generator = new LambdaHandlerGenerator
    val classDef = generator.generateLambdaHandlerClass(partitionInstance, className)
    val fileContents =
      s"""package $packageName
         |
         |$classDef
         |""".stripMargin
    val fileBytes = fileContents.getBytes(StandardCharsets.UTF_8)

    this.logger.info(s"Writing file '$scalaFile'")

    Files.createDirectories(scalaFile.getParent)
    Files.write(scalaFile, fileBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }

  private def compilePartitionCdkFile(partitionInstance: ApplicationInstance,
                                      packageName: String,
                                      className: String,
                                      cdkFile: Path,
                                      params: InstanceParameters): Unit = {
    val jarPath = params.getValue("code")
    val cdkPackage = params.getValueOrDefault("cdk", "aws-cdk-lib")
    val generator = new LambdaCdkGenerator
    val fileContents = generator.generateLambdaDefinition(partitionInstance, jarPath, packageName, className, cdkPackage)
    val fileBytes = fileContents.getBytes(StandardCharsets.UTF_8)

    this.logger.info(s"Writing file '$cdkFile'")

    Files.write(cdkFile, fileBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }
}
