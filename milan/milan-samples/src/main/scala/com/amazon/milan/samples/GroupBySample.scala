package com.amazon.milan.samples

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.application.sinks.SingletonMemorySink
import com.amazon.milan.application.sources.ListDataSource
import com.amazon.milan.flink.compiler.FlinkCompiler
import com.amazon.milan.lang._
import com.amazon.milan.lang.aggregation._
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory


object GroupBySample {
  def main(args: Array[String]): Unit = {
    val input = Stream.of[KeyValueRecord].withName("input")

    // Group by the "key" field and output a tuple stream with two fields.
    // One field is the group key, the other is the sum of the "value" field for all group members.
    val output =
    input
      .groupBy(r => r.key)
      .select(
        ((key: Int, _: KeyValueRecord) => key) as "key",
        ((_: Int, r: KeyValueRecord) => sum(r.value)) as "sum")
      .withName("output")

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()

    // Create some sample data for the input streams.
    val inputRecords = List((1, 1), (1, 2), (2, 5), (3, 6), (2, 3), (3, 1)).map(t => KeyValueRecord(t._1, t._2))
    config.setSource(input, new ListDataSource(inputRecords))

    val outputSink = new SingletonMemorySink[output.RecordType]()
    config.addSink(output, outputSink)

    // Compile and run.
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    FlinkCompiler.defaultCompiler.compile(graph, config, env)

    env.execute()

    // Sometimes it takes a second for the records to arrive at the sink after the execution finishes.
    Thread.sleep(1000)

    val logger = Logger(LoggerFactory.getLogger(getClass))

    logger.info("Output Records:")
    outputSink.getValues.foreach(r => logger.info(r.toString()))
  }
}
