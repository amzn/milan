package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.SemanticVersion
import com.amazon.milan.application.sinks.{DynamoDbTableSink, SqsDataSink}
import com.amazon.milan.application.sources.{DynamoDbStreamSource, SqsDataSource}
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.compiler.scala.TypeLifter
import com.amazon.milan.compiler.scala.event.GeneratorOutputInfo
import com.amazon.milan.compiler.scala.testing.{IntRecord, KeyValueRecord}
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang._
import com.amazon.milan.tools.InstanceParameters
import org.junit.Assert.assertTrue
import org.junit.Test


@Test
class TestLambdaCdkGenerator {
  @Test
  def test_LambdaCdkGenerator_GenerateLambdaDefinition_WithDynamoDbSourceAndSink_GeneratesCode(): Unit = {
    val config = new ApplicationConfiguration()
    config.setSource("inputStream", new DynamoDbStreamSource[KeyValueRecord]("inputTable"))
    config.addSink("outputStream", new DynamoDbTableSink[KeyValueRecord]("outputTableSink", "outputTable"))

    val instance = new ApplicationInstance(
      "instanceId",
      new Application("appId", StreamCollection.build(), SemanticVersion.ZERO),
      config
    )

    val handlerGeneratorOutput = GeneratorOutputInfo("", "", List.empty, List.empty, List.empty, List.empty)
    val generator = new LambdaCdkGenerator()
    val lambdaDef = generator.generateLambdaDefinition(instance, handlerGeneratorOutput, "/home/test.jar", "com.test", "HandlerClass", "aws-sdk-lib")
    val cdkFile = lambdaDef.cdkFileContents

    assertTrue(cdkFile.contains(""""EventSourceArnPrefix_inputStream": `${props.inputStreamInputTable.tableStreamArn}`"""))
  }

  @Test
  def test_LambdaCdkGenerator_GenerateLambdaDefinition_WithSqsSourceAndSink_GeneratesCode(): Unit = {
    val config = new ApplicationConfiguration()
    config.setSource("inputStream", new SqsDataSource[KeyValueRecord]("inputQueue"))
    config.addSink("outputStream", new SqsDataSink[KeyValueRecord]("outputSink", "outputQueue"))

    val instance = new ApplicationInstance(
      "instanceId",
      new Application("appId", StreamCollection.build(), SemanticVersion.ZERO),
      config
    )

    val handlerGeneratorOutput = GeneratorOutputInfo("", "", List.empty, List.empty, List.empty, List.empty)
    val generator = new LambdaCdkGenerator()
    val lambdaDef = generator.generateLambdaDefinition(instance, handlerGeneratorOutput, "/home/test.jar", "com.test", "HandlerClass", "aws-sdk-lib")
    val cdkFile = lambdaDef.cdkFileContents

    assertTrue(cdkFile.contains(""""EventSourceArnPrefix_inputStream": `${props.inputStreamInputQueue.queueArn}`"""))
  }

  @Test
  def test_LambdaCdkGenerator_GenerateLambdaDefinition_AfterHandlerGeneration_GeneratesCodeWithExpectedSnippets(): Unit = {
    val input = Stream.of[KeyValueRecord].withId("input")
    val output = input.map(r => KeyValueRecord(r.key, r.value + 1)).withId("output")

    val streams = StreamCollection.build(output)
    val app = new Application("TestApp", streams, SemanticVersion.ZERO)

    val config = new ApplicationConfiguration()
    config.setSource(input, new SqsDataSource[KeyValueRecord]("inputQueue"))
    config.addSink(output, new SqsDataSink[KeyValueRecord]("OutputSink", "outputQueue"))

    val instance = new ApplicationInstance(app, config)

    val handlerGenerator = new LambdaHandlerGenerator
    val plugin = new AwsServerlessGeneratorPlugin(TypeLifter.createDefault(), InstanceParameters.empty)
    val handlerGeneratorOutput = handlerGenerator.generateLambdaHandlerClass(instance, plugin)

    val cdkGenerator = new LambdaCdkGenerator
    val lambdaDef = cdkGenerator.generateLambdaDefinition(instance, handlerGeneratorOutput.handlerGeneratorOutput, "/home/test.jar", "com.test", "HandlerClass", "test-sdk")
    val cdkFile = lambdaDef.cdkFileContents

    assertTrue(cdkFile.contains("props.inputInputQueue.queueArn"))
    assertTrue(cdkFile.contains("props.outputOutputQueue.queueUrl"))
  }

  @Test
  def test_LambdaCdkGenerator_GenerateLambdaDefinition_WithDynamoDbStateStore_GeneratesTableConstruct(): Unit = {
    val input = Stream.of[IntRecord].withId("input")
    val output = input.maxBy(r => r.i).withId("output")

    val streams = StreamCollection.build(output)
    val app = new Application("TestApp", streams, SemanticVersion.ZERO)

    val config = new ApplicationConfiguration()
    config.setSource(input, new DynamoDbStreamSource[IntRecord]())
    config.addSink(output, new DynamoDbTableSink[IntRecord]("Output"))

    val instance = new ApplicationInstance(app, config)

    val handlerGenerator = new LambdaHandlerGenerator
    val plugin = new AwsServerlessGeneratorPlugin(TypeLifter.createDefault(), InstanceParameters.empty)
    val handlerGeneratorOutput = handlerGenerator.generateLambdaHandlerClass(instance, plugin)

    val cdkGenerator = new LambdaCdkGenerator
    val lambdaDef = cdkGenerator.generateLambdaDefinition(instance, handlerGeneratorOutput.handlerGeneratorOutput, "/home/test.jar", "com.test", "HandlerClass", "test-sdk")
    val cdkFile = lambdaDef.cdkFileContents

    assertTrue(cdkFile.contains("const output_state_stateTable = new dynamodb.Table(scope"))
    assertTrue(cdkFile.contains(""""output_state_TableName": `${output_state_stateTable.tableName}`"""))
  }

  @Test
  def test_LambdaCdkGenerator_WithPartitionedApplication_AddsLambdaPropsToApplicationPropsWithDefaults(): Unit = {
    val input = Stream.of[IntRecord].withId("input")
    val max = input.maxBy(r => r.i).withId("max")
    val min = max.minBy(r => r.i).withId("min")

    val streams = StreamCollection.build(min)
    val app = new Application("TestApp", streams, SemanticVersion.ZERO)

    val config = new ApplicationConfiguration()
    config.setSource(input, new DynamoDbStreamSource[IntRecord]())
    config.addSink(min, new DynamoDbTableSink[IntRecord]("Output"))

    val instance = new ApplicationInstance(app, config)
    val partitions = ApplicationPartitioner.partitionApplication(instance, new SqsInputOutputMappingProcessor)

    val handlerGenerator = new LambdaHandlerGenerator
    val plugin = new AwsServerlessGeneratorPlugin(TypeLifter.createDefault(), InstanceParameters.empty)

    val partitionOutputs = partitions.map(partition => (partition, handlerGenerator.generateLambdaHandlerClass(partition, plugin)))

    val lambdaCdkGenerator = new LambdaCdkGenerator
    val lambdaCdkOutputs = partitionOutputs.map {
      case (partition, handler) =>
        val lambdaCdkOutput = lambdaCdkGenerator.generateLambdaDefinition(partition, handler.handlerGeneratorOutput, "test.jar", "test", "TestClass", "cdk-test")
        (partition, lambdaCdkOutput)
    }

    val lambdaCdkPartitions = lambdaCdkOutputs.map {
      case (partition, lambdaCdkOutput) =>
        ApplicationPartitionCdkConstructInfo(partition, "test.ts", lambdaCdkOutput)
    }

    val appCdkGenerator = new ApplicationCdkGenerator
    val appCdkFile = appCdkGenerator.generatePartitionedApplicationCreationFile("TestApp", lambdaCdkPartitions, "cdk-test")
    assertTrue(appCdkFile.contains("readonly default_memorySize?: number;"))
    assertTrue(appCdkFile.contains("readonly default_timeout?: cdk.Duration;"))
    assertTrue(appCdkFile.contains("readonly TestApp_max_timeout?: cdk.Duration;"))
    assertTrue(appCdkFile.contains("readonly TestApp_max_memorySize?: number;"))
    assertTrue(appCdkFile.contains("readonly TestApp_min_timeout?: cdk.Duration;"))
    assertTrue(appCdkFile.contains("readonly TestApp_min_memorySize?: number;"))
    assertTrue(appCdkFile.contains("timeout: props.default_timeout ? props.default_timeout : props.TestApp_max_timeout,"))
    assertTrue(appCdkFile.contains("memorySize: props.default_memorySize ? props.default_memorySize : props.TestApp_max_memorySize,"))
    assertTrue(appCdkFile.contains("timeout: props.default_timeout ? props.default_timeout : props.TestApp_min_timeout,"))
    assertTrue(appCdkFile.contains("memorySize: props.default_memorySize ? props.default_memorySize : props.TestApp_min_memorySize,"))
  }
}
