package com.amazon.milan.samples.aws

import com.amazon.milan.application.sinks.SqsDataSink
import com.amazon.milan.application.sources.SqsDataSource
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang._
import com.amazon.milan.tools.{ApplicationInstanceProvider, InstanceParameters}
import com.amazon.milan.{Id, SemanticVersion}


class SqsInputRecord(val recordId: String, val value: Int) {
  override def toString: String = s"${this.value}"

  def this(value: Int) {
    this(Id.newId(), value)
  }

  override def equals(obj: Any): Boolean = obj match {
    case i: SqsInputRecord => this.value == i.value
    case _ => false
  }
}


class SqsOutputRecord(val recordId: String, val value: Int) {
  def this(value: Int) {
    this(Id.newId(), value)
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: SqsOutputRecord => this.value == o.value
    case _ => false
  }
}

class SqsSample extends ApplicationInstanceProvider {
  override def getApplicationInstance(params: InstanceParameters): ApplicationInstance = {
    val input = Stream.of[SqsInputRecord].withId("input")
    val output = input.map(r => new SqsOutputRecord(r.value + 10)).withId("output")

    val streams = StreamCollection.build(output)

    val application = new Application("SqsSample", streams, SemanticVersion.ZERO)

    val config = new ApplicationConfiguration()

    // By not specifying the table names here, we require they be read from the environment variables set
    // in the generated CDK construct.
    config.setSource(input, new SqsDataSource[SqsInputRecord]())
    config.addSink(output, new SqsDataSink[SqsOutputRecord]("Output"))

    val instance = new ApplicationInstance(application, config)

    instance
  }
}
