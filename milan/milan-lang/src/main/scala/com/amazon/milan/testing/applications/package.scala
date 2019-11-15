package com.amazon.milan.testing

import com.amazon.milan.Id
import com.amazon.milan.application.sinks.SingletonMemorySink
import com.amazon.milan.application.sources.ListDataSource
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.lang.{Stream, StreamGraph}
import com.amazon.milan.types.LineageRecord
import com.amazon.milan.typeutil.{ObjectTypeDescriptor, TypeDescriptor}

import scala.language.implicitConversions


package object applications {

  implicit class ApplicationConfigurationExtensions(config: ApplicationConfiguration) {
    def setListSource[T: TypeDescriptor](stream: Stream[T, _], values: T*): Unit = {
      this.config.setSource(stream, new ListDataSource[T](values.toList))
    }

    def setListSource[T: TypeDescriptor](streamId: String, values: T*): Unit = {
      this.config.setSource(streamId, new ListDataSource[T](values.toList))
    }

    def addMemorySink[T: TypeDescriptor](stream: Stream[T, _]): SingletonMemorySink[T] = {
      val sink = new SingletonMemorySink[T]()
      this.config.addSink(stream, sink)
      sink
    }

    def addMemorySink[T: TypeDescriptor](streamId: String): SingletonMemorySink[T] = {
      val sink = new SingletonMemorySink[T]()
      this.config.addSink(streamId, sink)
      sink
    }

    def addMemoryLineageSink(): SingletonMemorySink[LineageRecord] = {
      val lineageRecordTypeDescriptor = new ObjectTypeDescriptor[LineageRecord]("com.amazon.milan.types.LineageRecord", List(), List())
      val sink = new SingletonMemorySink[LineageRecord]()(lineageRecordTypeDescriptor)
      this.config.addLineageSink(sink)
      sink
    }
  }

  /**
   * Gets a JSON string containing a packaged application and config.
   */
  def packageApplication(graph: StreamGraph, config: ApplicationConfiguration): String = {
    val application = new Application(graph)
    val instance = new ApplicationInstance(Id.newId(), application, config)
    instance.toJsonString
  }
}
