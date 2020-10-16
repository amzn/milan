package com.amazon.milan.compiler.scala.event

import com.amazon.milan.application.sinks.SingletonMemorySink
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.compiler.scala.RuntimeEvaluator
import com.amazon.milan.lang
import com.amazon.milan.lang.StreamGraph


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
    val graph = new StreamGraph(outputStream)
    this.compile(graph, config)
  }

  def compile(graph: StreamGraph, config: ApplicationConfiguration): RecordConsumer = {
    val instance = new ApplicationInstance(new Application(graph), config)
    val className = "TestClass"
    val classDef = EventHandlerClassGenerator.generateClass(instance, className)

    val code =
      s"""
         |$classDef
         |
         |new $className
         |""".stripMargin

    RuntimeEvaluator.default.eval[RecordConsumer](code)
  }
}
