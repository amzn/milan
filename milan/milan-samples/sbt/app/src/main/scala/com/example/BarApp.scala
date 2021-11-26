package com.example

import com.amazon.milan.application.sinks.LogSink
import com.amazon.milan.application.sources.ListDataSource
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.graph._
import com.amazon.milan.lang._
import com.amazon.milan.tools._
import com.amazon.milan.typeutil._
import com.amazon.milan.SemanticVersion


class BarApp extends ApplicationInstanceProvider {
  override def getApplicationInstance(params: List[(String, String)]): ApplicationInstance = {
    val stream = Stream.of[Record]
    val output = stream.map(r => Record(r.value + 2))

    val streams = StreamCollection.build(output)

    val config = new ApplicationConfiguration()
    config.setSource(stream, new ListDataSource[Record](List(Record(1), Record(2))))
    config.addSink(output, new LogSink[Record]())

    new ApplicationInstance(
      new Application("BarApp", streams, SemanticVersion.ZERO),
      config)
  }
}
