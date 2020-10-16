package com.amazon.milan.compiler.scala

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.application.sinks.SingletonMemorySink
import com.amazon.milan.lang._


package object event {

  implicit class EventHandlerCompilerTestingApplicationConfigurationExtensions(applicationConfiguration: ApplicationConfiguration) {
    def addMemorySink[T](stream: Stream[T]): SingletonMemorySink[T] = {
      val sink = new SingletonMemorySink[T]()(stream.recordType)
      this.applicationConfiguration.addSink(stream, sink)
      sink
    }
  }

}
