package com.amazon.milan.samples

import com.amazon.milan.SemanticVersion
import com.amazon.milan.application.sinks.LogSink
import com.amazon.milan.application.sources.ListDataSource
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.lang._
import com.amazon.milan.lang.aggregation._
import com.amazon.milan.tools.ApplicationInstanceProvider


class GroupByFlatMapSample extends ApplicationInstanceProvider {
  override def getApplicationInstance(params: List[(String, String)]): ApplicationInstance = {
    val input = Stream.of[KeyValueRecord].withName("input")

    // Group by the "key" field and output a stream with the sum of the "value" field for each key.
    // Due to limitations of the Milan Scala interpreter, the function used inside flatMap can only be a function call.
    // Otherwise we could write:
    // flatMap((key, group) => group.sumBy(...))

    def sumByValue(group: Stream[KeyValueRecord]): Stream[KeyValueRecord] =
      group.sumBy(r => r.value, (r, sum) => KeyValueRecord(r.key, sum))

    val output = input
      .groupBy(r => r.key).withName("grouped")
      .flatMap((key, group) => sumByValue(group)).withName("output")

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()

    // Create some sample data for the input streams.
    val inputRecords = List((1, 1), (1, 2), (2, 5), (3, 6), (2, 3), (3, 1)).map(t => KeyValueRecord(t._1, t._2))
    config.setSource(input, new ListDataSource(inputRecords))

    val outputSink = new LogSink[output.RecordType]()
    config.addSink(output, outputSink)

    new ApplicationInstance(
      new Application("GroupByFlatMapSample", graph, SemanticVersion(1, 0, 0)),
      config)
  }
}


class GroupBySelectSample extends ApplicationInstanceProvider {
  override def getApplicationInstance(params: List[(String, String)]): ApplicationInstance = {
    val input = Stream.of[KeyValueRecord].withName("input")

    // Group by the "key" field and output a tuple stream with two fields.
    // One field is the group key, the other is the sum of the "value" field for all group members.
    val output =
    input
      .groupBy(r => r.key)
      .select((key, r) => fields(field("key", key), field("sum", sum(r.value))))
      .withName("output")

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()

    // Create some sample data for the input streams.
    val inputRecords = List((1, 1), (1, 2), (2, 5), (3, 6), (2, 3), (3, 1)).map(t => KeyValueRecord(t._1, t._2))
    config.setSource(input, new ListDataSource(inputRecords))

    val outputSink = new LogSink[output.RecordType]()
    config.addSink(output, outputSink)

    new ApplicationInstance(
      new Application("GroupBySelectSample", graph, SemanticVersion.ZERO),
      config)
  }
}
