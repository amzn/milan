package com.amazon.milan.flink.control

import java.nio.file.{Files, Paths}
import java.time.Duration
import java.util.Properties

import com.amazon.milan.control.{ApplicationControllerMessageEnvelope, ApplicationControllerState}
import com.amazon.milan.flink.apps.{ArgumentsBase, NamedArgument}
import com.amazon.milan.flink.serialization.{JsonDeserializationSchema, JsonSerializationSchema}
import com.amazon.milan.manage.ProcessCommandExecutor
import com.amazon.milan.storage.aws.{Repositories, S3Util}
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.connectors.kinesis.config.{AWSConfigConstants, ConsumerConfigConstants}
import org.apache.flink.streaming.connectors.kinesis.{FlinkKinesisConsumer, FlinkKinesisProducer}
import org.slf4j.LoggerFactory


object ControllerFlinkApp {

  def main(args: Array[String]): Unit = {
    val logger = Logger(LoggerFactory.getLogger(getClass))

    val params = new CmdArgs()
    params.parse(args)

    val kinesisConsumerConfig = new Properties()
    kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION, params.region)
    kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, AWSConfigConstants.CredentialProvider.AUTO.toString)
    kinesisConsumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, ConsumerConfigConstants.InitialPosition.LATEST.toString)

    logger.info(s"Consuming messages from kinesis stream '${params.messageInputKinesisStreamName}'.")
    val messageSource = new FlinkKinesisConsumer[ApplicationControllerMessageEnvelope](
      params.messageInputKinesisStreamName,
      new JsonDeserializationSchema[ApplicationControllerMessageEnvelope](),
      kinesisConsumerConfig)

    val kinesisProducerConfig = new Properties()
    kinesisProducerConfig.setProperty(AWSConfigConstants.AWS_REGION, params.region)
    kinesisProducerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, AWSConfigConstants.CredentialProvider.AUTO.toString)

    logger.info(s"Writing state updates to kinesis stream '${params.stateOutputKinesisStreamName}'.")
    val stateSink = new FlinkKinesisProducer[ApplicationControllerState](
      new JsonSerializationSchema[ApplicationControllerState](),
      kinesisProducerConfig)
    stateSink.setDefaultStream(params.stateOutputKinesisStreamName)
    stateSink.setDefaultPartition("0")

    val diagnosticSink = new FlinkKinesisProducer[ApplicationControllerDiagnostic](
      new JsonSerializationSchema[ApplicationControllerDiagnostic](),
      kinesisProducerConfig)
    diagnosticSink.setDefaultStream(params.diagnosticOutputKinesisStreamName)
    diagnosticSink.setDefaultPartition("0")

    val packageCacheFolder = Paths.get(params.packageCacheFolder)
    logger.info(s"Creating package cache folder '$packageCacheFolder'.")
    Files.createDirectories(packageCacheFolder)

    val (bucket, key) = S3Util.parseS3Uri(params.packageRepositoryS3Location)
    val packageRepository = Repositories.createS3PackageRepository(bucket, key)

    // Use yarn client mode (i.e. cluster mode) if specified in the arguments.
    val flinkClient =
      if (params.yarnApplication) {
        logger.info("Finding Flink Yarn application.")
        val yarnApplicationId = this.getFlinkYarnApplicationId()
        logger.info(s"Flink running with Yarn application ID '$yarnApplicationId'.")

        new CommandLineFlinkYarnClient(new ProcessCommandExecutor(), yarnApplicationId)
      }
      else {
        new CommandLineFlinkClient(new ProcessCommandExecutor())
      }

    val applicationController = new ApplicationController(packageRepository, flinkClient, params.packageCacheFolder,
      params.region, params.cloudFormationCacheFolder)
    val applicationMessageHandler = new ApplicationControllerMessageHandler(applicationController)
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    logger.info("Executing controller Flink application.")
    execute(applicationMessageHandler, env, messageSource, stateSink, diagnosticSink, Duration.ofSeconds(10), params.snapshotDirectory, params.snapshotIntervalInMinutes)
  }

  /**
   * Executes the controller Flink application, sending messages to a specified [[ApplicationController]] instance.
   *
   * @param applicationController       The application controller.
   * @param env                         The streaming environment.
   * @param messageSource               The source of controller messages.
   * @param stateSink                   The destination for controller state.
   * @param diagnosticsSink             The destination for controller diagnostics.
   * @param applicationStateCheckPeriod The period between checking the state of running applications.
   */
  def execute(applicationController: ApplicationControllerMessageHandler,
              env: StreamExecutionEnvironment,
              messageSource: SourceFunction[ApplicationControllerMessageEnvelope],
              stateSink: SinkFunction[ApplicationControllerState],
              diagnosticsSink: SinkFunction[ApplicationControllerDiagnostic],
              applicationStateCheckPeriod: Duration,
              snapshotDirectory: String,
              snapshotIntervalInMinutes: Int): Unit = {
    env.setParallelism(1)
    val messages = env.addSource(messageSource)

    val diagnosticOutputTag = new OutputTag[ApplicationControllerDiagnostic]("diagnostics")

    val processor = new ApplicationControllerProcessFunction(applicationController, applicationStateCheckPeriod, diagnosticOutputTag, snapshotDirectory, snapshotIntervalInMinutes)

    // We can only use .process() on a keyed stream, so we have to key by something.
    // We could use a dummy KeyExtractor that returns a constant value, which would send all messages
    // to the same processor instance.
    // Instead we key by a controllerId field, which allows us to have an essentially unlimited number of "virtual"
    // application controllers. Every value of controllerId will have a different instance of the processor associated
    // with it and will be responsible only for the Milan applications started by that instance.
    val processorOutput = messages.keyBy("controllerId").process(processor)
    processorOutput.addSink(stateSink)

    val diagnostics = processorOutput.getSideOutput(diagnosticOutputTag)
    diagnostics.addSink(diagnosticsSink)

    env.execute()
  }

  /**
   * Gets the Yarn application ID of the Flink cluster.
   *
   * @return The ID of the Flink Yarn application.
   */
  private def getFlinkYarnApplicationId(): String = {
    val output = ProcessCommandExecutor.execute("yarn application -list")

    if (output.exitCode != 0) {
      throw new Exception(s"Error getting Flink Yarn application ID. Error output:\n${output.getFullErrorOutput}")
    }

    output.standardOutput.find(line => line.contains("Apache Flink")) match {
      case None =>
        throw new Exception(s"Flink Yarn application not found.")

      case Some(flinkLine) =>
        flinkLine.split("\\s")(0)
    }
  }

  class CmdArgs extends ArgumentsBase {
    @NamedArgument(Name = "message-input-stream", ShortName = "m")
    var messageInputKinesisStreamName: String = ""

    @NamedArgument(Name = "state-output-stream", ShortName = "s")
    var stateOutputKinesisStreamName: String = ""

    @NamedArgument(Name = "diagnostic-output-stream", ShortName = "d")
    var diagnosticOutputKinesisStreamName: String = ""

    @NamedArgument(Name = "package-cache-folder", ShortName = "c")
    var packageCacheFolder: String = ""

    @NamedArgument(Name = "s3-package-repo", ShortName = "p")
    var packageRepositoryS3Location: String = ""

    @NamedArgument(Name = "yarn", ShortName = "y", Required = false, IsFlag = true, DefaultValue = "false")
    var yarnApplication: Boolean = _

    @NamedArgument(Name = "region", ShortName = "r", Required = false, DefaultValue = "eu-west-1")
    var region: String = ""

    @NamedArgument(Name = "snapshot-directory", ShortName = "sd", Required = false, DefaultValue = "s3://ml-cam-storage/doa/snapshots")
    var snapshotDirectory: String = ""

    @NamedArgument(Name = "snapshot-interval-in-minutes", ShortName = "si", Required = false, DefaultValue = "60")
    var snapshotIntervalInMinutes: Int = _

    @NamedArgument(Name = "cloudformation-cache-folder", ShortName = "cf", Required = false, DefaultValue = "/tmp/milan-cloudformation")
    var cloudFormationCacheFolder: String = ""
  }

}
