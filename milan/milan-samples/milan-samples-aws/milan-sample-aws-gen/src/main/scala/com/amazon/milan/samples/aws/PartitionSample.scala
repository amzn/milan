package com.amazon.milan.samples.aws

import com.amazon.milan.application.sinks.DynamoDbTableSink
import com.amazon.milan.application.sources.DynamoDbStreamSource
import com.amazon.milan.{Id, SemanticVersion}
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.compiler.scala.testing.IntRecord
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang._
import com.amazon.milan.tools.{ApplicationInstanceProvider, InstanceParameters}


class PartitionSample extends ApplicationInstanceProvider {
  override def getApplicationInstance(params: InstanceParameters): ApplicationInstance = {
    val input = Stream.of[IntRecord].withId("input")
    val runningSum = input.sumBy(r => r.i, (_, sum) => IntRecord(sum)).withId("sum")
    val output = runningSum.maxBy(r => r.i).withId("max")

    val streams = StreamCollection.build(output)

    val application = new Application("PartitionSample", streams, SemanticVersion.ZERO)

    val config = new ApplicationConfiguration()

    // By not specifying the table names here, we require they be read from the environment variables set
    // in the generated CDK construct.
    config.setSource(input, new DynamoDbStreamSource[IntRecord]())
    config.addSink(output, new DynamoDbTableSink[IntRecord]("outputTableSink"))

    val instance = new ApplicationInstance(application, config)

    instance
  }
}
