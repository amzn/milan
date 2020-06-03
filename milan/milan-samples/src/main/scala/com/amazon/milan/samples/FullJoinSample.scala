package com.amazon.milan.samples

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.application.sinks.SingletonMemorySink
import com.amazon.milan.application.sources.ListDataSource
import com.amazon.milan.lang._


object FullJoinSample {
  def main(args: Array[String]): Unit = {
    val left = Stream.of[KeyValueRecord].withName("left")
    val right = Stream.of[KeyValueRecord].withName("right")

    // Join the two streams using the condition that the keys are equal.
    // This is a "full join" which means an output record will be produced any time
    // a new record arrives on either stream.
    val joined = left.fullJoin(right).where((l, r) => l.key == r.key)

    // Define the output of the join.
    // The input to the functions provided in the select statement are the latest records to arrive on the input streams.
    // Note that at this point either of the input records could be null if no records have arrived on those streams.
    // We could prevent this by adding "l != null && r != null" to the join condition above.
    val output = joined.select((l, r) => fields(
      field("key", if (l == null) r.key else l.key),
      field("left", l),
      field("right", r)
    )).withName("output")

    // Create a stream graph, passing in the output stream. Upstream dependencies are added automatically.
    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()

    // Create some sample data for the input streams.
    val leftRecords = List((1, 1), (1, 2), (2, 3), (2, 4)).map(t => KeyValueRecord(t._1, t._2))
    config.setSource(left, new ListDataSource(leftRecords))

    val rightRecords = List((2, 1), (1, 3), (2, 2), (1, 5)).map(t => KeyValueRecord(t._1, t._2))
    config.setSource(right, new ListDataSource(rightRecords))

    val outputSink = new SingletonMemorySink[output.RecordType]()
    config.addSink(output, outputSink)

    // TODO: compile and execute.
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    FlinkCompiler.defaultCompiler.compile(graph, config, env)
    //
    //    env.execute()

    // Sometimes it takes a second for the records to arrive at the sink after the execution finishes.
    //    Thread.sleep(1000)

    //    val logger = Logger(LoggerFactory.getLogger(getClass))

    //    logger.info("Output Records:")
    //    outputSink.getValues.foreach(r => logger.info(r.toString()))
  }
}
