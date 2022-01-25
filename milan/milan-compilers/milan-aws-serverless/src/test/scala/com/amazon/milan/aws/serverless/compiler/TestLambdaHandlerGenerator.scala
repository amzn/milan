package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.sinks.SingletonMemorySink
import com.amazon.milan.application.sources.{DynamoDbStreamSource, SqsDataSource}
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.aws.serverless.compiler.TestLambdaHandlerGenerator.HandlerFactory
import com.amazon.milan.aws.serverless.runtime.{EventSourceArnAccessor, MapEventSourceArnAccessor, MilanLambdaHandler}
import com.amazon.milan.compiler.scala._
import com.amazon.milan.compiler.scala.testing.IntRecord
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang._
import com.amazon.milan.serialization.MilanObjectMapper
import org.apache.commons.io.output.ByteArrayOutputStream
import org.junit.Assert._
import org.junit.Test

import java.io.{ByteArrayInputStream, InputStream, OutputStreamWriter}

object TestLambdaHandlerGenerator {
  trait HandlerFactory {
    def createHandler(arns: EventSourceArnAccessor): MilanLambdaHandler
  }
}


@Test
class TestLambdaHandlerGenerator {
  @Test
  def test_LambdaHandlerGenerator_GenerateLambdaHandlerClass_WithDynamoDbInput_MapsInputRecordFromEvent(): Unit = {
    val input = Stream.of[IntRecord].withId("input")
    val output = input.map(r => IntRecord(r.i + 1))

    val streams = StreamCollection.build(output)
    val application = new Application(streams)

    val config = new ApplicationConfiguration()
    val sink = new SingletonMemorySink[IntRecord]
    config.addSink(output, sink)
    val source = new DynamoDbStreamSource[IntRecord]("streamArn")
    config.setSource(input, source)
    val instance = new ApplicationInstance(application, config)

    val handler = this.createHandlerInstance(instance, "input" -> "streamArn")

    val recordJson = s"""{"recordId": {"S": "id"}, "i": {"N": "2"}}"""
    val eventStream = this.createDynamoDbLambdaInputStream("streamArn:12345", recordJson)

    handler.handleRequest(eventStream, new ByteArrayOutputStream(), new TestLambdaContext)

    assertEquals(IntRecord(3), sink.getValues.last)
  }

  @Test
  def test_LambdaHandlerGenerator_GenerateLambdaHandlerClass_WithSqsInput_MapsInputRecordFromEvent(): Unit = {
    val input = Stream.of[IntRecord].withId("input")
    val output = input.map(r => IntRecord(r.i + 1))

    val streams = StreamCollection.build(output)
    val application = new Application(streams)

    val config = new ApplicationConfiguration()
    val sink = new SingletonMemorySink[IntRecord]
    config.addSink(output, sink)
    val source = new SqsDataSource[IntRecord]("queueArn")
    config.setSource(input, source)
    val instance = new ApplicationInstance(application, config)

    val handler = this.createHandlerInstance(instance, "input" -> "queueArn")

    val recordJson = s"""{"recordId": "id", "i": 2}"""
    val eventStream = this.createSqsLambdaInputStream("queueArn:12345", recordJson)
    handler.handleRequest(eventStream, new ByteArrayOutputStream(), new TestLambdaContext)

    assertEquals(IntRecord(3), sink.getValues.last)
  }

  private def createHandlerInstance(applicationInstance: ApplicationInstance,
                                    inputEventSourceArns: (String, String)*): MilanLambdaHandler = {
    val generator = new LambdaHandlerGenerator()
    val handlerClassName = "EventHandler"
    val generatedCode = generator.generateLambdaHandlerClass(applicationInstance, handlerClassName)

    val codeToEval =
      s"""
         |$generatedCode
         |
         |new com.amazon.milan.aws.serverless.compiler.TestLambdaHandlerGenerator.HandlerFactory {
         |  override def createHandler(arns: com.amazon.milan.aws.serverless.runtime.EventSourceArnAccessor): com.amazon.milan.aws.serverless.runtime.MilanLambdaHandler = {
         |    new $handlerClassName(arns)
         |  }
         |}
         |""".stripMargin

    val handlerFactory = RuntimeEvaluator.default.eval[HandlerFactory](codeToEval)

    handlerFactory.createHandler(new MapEventSourceArnAccessor(inputEventSourceArns: _*))
  }

  private def createSqsLambdaInputStream(eventSourceArn: String, body: String): InputStream = {
    val jsonString = MilanObjectMapper.writeValueAsString(body)
    val snippet = s""" "body": $jsonString """
    this.createLambdaInputStream("aws:sqs", eventSourceArn, snippet)
  }

  private def createDynamoDbLambdaInputStream(eventSourceArn: String, recordJson: String): InputStream = {
    val snippet =
      s"""
         |"dynamodb": {
         |  "NewImage": ${recordJson.indentTail(4)}
         |}
         |""".stripMargin

    this.createLambdaInputStream("aws:dynamodb", eventSourceArn, snippet)
  }

  private def createLambdaInputStream(eventSource: String,
                                      eventSourceArn: String,
                                      recordSnippet: String): InputStream = {
    val eventJson =
      s"""
         |{
         |  "Records": [
         |    {
         |      "eventSource": "$eventSource",
         |      "eventSourceARN": "$eventSourceArn",
         |      ${recordSnippet.indentTail(3)}
         |    }
         |  ]
         |}
         |""".stripMargin

    val outputStream = new ByteArrayOutputStream()
    val writer = new OutputStreamWriter(outputStream)
    writer.write(eventJson)
    writer.flush()

    val bytes = outputStream.toByteArray
    new ByteArrayInputStream(bytes)
  }
}
