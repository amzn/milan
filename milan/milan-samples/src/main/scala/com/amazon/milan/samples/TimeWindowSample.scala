package com.amazon.milan.samples

import java.time.{Duration, Instant}

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.application.sinks.SingletonMemorySink
import com.amazon.milan.application.sources.ListDataSource
import com.amazon.milan.lang._
import com.amazon.milan.lang.aggregation._


object TimeWindowSample {
  def main(args: Array[String]): Unit = {
    val input = Stream.of[DateValueRecord]

    // Create sliding windows that are five seconds long and start every second.
    // In each window compute the sum of the record values.
    val output =
    input
      .slidingWindow(r => r.dateTime, Duration.ofSeconds(5), Duration.ofSeconds(1), Duration.ZERO)
      .select((windowStart, r) => fields(field("windowStart", windowStart), field("sum", sum(r.value))))

    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()

    // Create some sample data for the input streams.
    val now = Instant.now()
    val inputRecords = List.tabulate(10)(i => DateValueRecord(now.plusSeconds(i), 1))
    config.setSource(input, new ListDataSource(inputRecords))

    val outputSink = new SingletonMemorySink[output.RecordType]()
    config.addSink(output, outputSink)

    // TODO: compile and execute.
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    FlinkCompiler.defaultCompiler.compile(graph, config, env)

    //    env.execute()

    // Sometimes it takes a second for the records to arrive at the sink after the execution finishes.
    //    Thread.sleep(1000)

    //    val logger = Logger(LoggerFactory.getLogger(getClass))

    // Print the last output for each window.
    //    logger.info("Final Windows:")
    //    val finalWindows = outputSink.getValues
    //      .groupBy { case (windowStart, _) => windowStart }
    //      .map {
    //        case (windowStart, records) => (windowStart, records.last match {
    //          case (_, value) => value
    //        })
    //      }
    //      .toList
    //      .sortBy { case (windowStart, _) => windowStart }
    //    finalWindows.foreach(o => logger.info(o.toString()))
  }
}
