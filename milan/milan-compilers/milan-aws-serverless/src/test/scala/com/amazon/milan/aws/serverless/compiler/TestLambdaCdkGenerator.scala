package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.SemanticVersion
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.application.sinks.DynamoDbTableSink
import com.amazon.milan.application.sources.DynamoDbStreamSource
import com.amazon.milan.compiler.scala.testing.KeyValueRecord
import com.amazon.milan.graph.StreamCollection
import org.junit.Assert._
import org.junit.Test


@Test
class TestLambdaCdkGenerator {
  @Test
  def test_LambdaCdkGenerator_GenerateLambdaDefinition(): Unit = {
    val config = new ApplicationConfiguration()
    config.setSource("inputStream", new DynamoDbStreamSource[KeyValueRecord]("inputTable"))
    config.addSink("outputStream", new DynamoDbTableSink[KeyValueRecord]("outputTableSink", "outputTable"))

    val instance = new ApplicationInstance(
      "instanceId",
      new Application("appId", StreamCollection.build(), SemanticVersion.ZERO),
      config
    )

    val generator = new LambdaCdkGenerator()
    val lambdaDef = generator.generateLambdaDefinition(instance, "/home/test.jar", "com.test", "HandlerClass", "aws-sdk-lib")
    assertTrue(lambdaDef.nonEmpty)
  }
}
