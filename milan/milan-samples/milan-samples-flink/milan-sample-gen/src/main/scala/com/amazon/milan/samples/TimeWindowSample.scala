package com.amazon.milan.samples

import com.amazon.milan.SemanticVersion
import com.amazon.milan.application.sinks.LogSink
import com.amazon.milan.application.sources.ListDataSource
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang._
import com.amazon.milan.lang.aggregation._
import com.amazon.milan.tools.{ApplicationInstanceProvider, InstanceParameters}

import java.time.{Duration, Instant}


class TimeWindowSample extends ApplicationInstanceProvider {
  override def getApplicationInstance(params: InstanceParameters): ApplicationInstance = {
    val input = Stream.of[DateValueRecord]

    // Create sliding windows that are five seconds long and start every second.
    // In each window compute the sum of the record values.
    val output =
    input
      .slidingWindow(r => r.dateTime, Duration.ofSeconds(5), Duration.ofSeconds(1), Duration.ZERO)
      .select((windowStart, r) => fields(field("windowStart", windowStart), field("sum", sum(r.value))))

    val streams = StreamCollection.build(output)

    val config = new ApplicationConfiguration()

    // Create some sample data for the input streams.
    val now = Instant.now()
    val inputRecords = List.tabulate(10)(i => DateValueRecord(now.plusSeconds(i), 1))
    config.setSource(input, new ListDataSource(inputRecords))

    val outputSink = new LogSink[output.RecordType]("printOutput")
    config.addSink(output, outputSink)

    new ApplicationInstance(
      new Application("TimeWindowSample", streams, SemanticVersion.ZERO),
      config)
  }
}
