package com.amazon.milan.compiler.scala.testing

import com.amazon.milan.application.sinks.SingletonMemorySink
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.compiler.scala.RuntimeEvaluator
import com.amazon.milan.compiler.scala.event.{EventHandlerClassGenerator, GeneratedStreams, RecordConsumer}
import com.amazon.milan.graph.{StreamCollection, typeCheckGraph}
import com.amazon.milan.lang
import org.apache.commons.io.output.ByteArrayOutputStream

object EventAppTester {
  def execute[TIn, TOut](outputStream: lang.Stream[TOut],
                         inputStream: lang.Stream[TIn],
                         inputRecords: TIn*): List[TOut] = {
    val sink = new SingletonMemorySink[TOut]()(outputStream.recordType)

    val config = new ApplicationConfiguration
    config.addSink(outputStream, sink)

    val compiledClass = this.compile(outputStream, config)

    inputRecords.foreach(record => compiledClass.consume(inputStream.streamName, record))

    sink.getValues
  }

  def compile(outputStream: lang.Stream[_], config: ApplicationConfiguration): RecordConsumer = {
    val streams = StreamCollection.build(outputStream)
    this.compile(streams, config)
  }

  def compile(streams: StreamCollection, config: ApplicationConfiguration): RecordConsumer = {
    val instance = new ApplicationInstance(new Application(streams), config)
    val className = "TestClass"
    val generatedClassInfo = EventHandlerClassGenerator.generateClass(instance, className)
    val classDef = generatedClassInfo.classDefinition

    val code =
      s"""
         |$classDef
         |
         |new $className
         |""".stripMargin

    RuntimeEvaluator.default.eval[RecordConsumer](code)
  }

  def compileStreams(streams: StreamCollection, config: ApplicationConfiguration): GeneratedStreams = {
    val instance = new ApplicationInstance(new Application(streams), config)
    val className = "TestClass"
    val output = new ByteArrayOutputStream()
    EventHandlerClassGenerator.generateClass(instance, className, output)
  }
}
