package com.amazon.milan.samples.aws

import com.amazon.milan.application.sinks.DynamoDbTableSink
import com.amazon.milan.application.sources.DynamoDbStreamSource
import com.amazon.milan.{Id, SemanticVersion}
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang._
import com.amazon.milan.tools.{ApplicationInstanceProvider, InstanceParameters}


class InputRecord(val recordId: String, val value: Int) {
  override def toString: String = s"${this.value}"

  def this(value: Int) {
    this(Id.newId(), value)
  }

  override def equals(obj: Any): Boolean = obj match {
    case i: InputRecord => this.value == i.value
    case _ => false
  }
}


class OutputRecord(val recordId: String, val value: Int) {
  def this(value: Int) {
    this(Id.newId(), value)
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: OutputRecord => this.value == o.value
    case _ => false
  }
}

class StatelessSample extends ApplicationInstanceProvider {
  override def getApplicationInstance(params: InstanceParameters): ApplicationInstance = {
    val input = Stream.of[InputRecord].withId("input")
    val output = input.map(r => new OutputRecord(r.value + 10)).withId("output")

    val streams = StreamCollection.build(output)

    val application = new Application("StatelessSample", streams, SemanticVersion.ZERO)

    val config = new ApplicationConfiguration()
    val inputTableName = params.getValue("inputTableName")
    val outputTableName = params.getValue("outputTableName")
    config.setSource(input, new DynamoDbStreamSource[InputRecord](inputTableName))
    config.addSink(output, new DynamoDbTableSink[OutputRecord]("outputTableSink", outputTableName))

    val instance = new ApplicationInstance(application, config)

    instance
  }
}
