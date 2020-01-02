package com.amazon.milan.flink

object FlinkTypeNames {
  def aggregateFunction(i: String, a: String, o: String) = s"org.apache.flink.api.common.functions.AggregateFunction[$i, $a, $o]"

  def connectedStreams(l: String, r: String) = s"org.apache.flink.streaming.api.datastream.ConnectedStreams[$l, $r]"

  def dataStream(t: String) = s"org.apache.flink.streaming.api.datastream.DataStream[$t]"

  def keyedStream(t: String, k: String) = s"org.apache.flink.streaming.api.datastream.KeyedStream[$t, $k]"

  def singleOutputStreamOperator(t: String) = s"org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator[$t]"

  val globalWindow = "org.apache.flink.streaming.api.windowing.windows.GlobalWindow"

  def sinkFunction(t: String) = s"org.apache.flink.streaming.api.functions.sink.SinkFunction[$t]"

  def typeInformation(t: String) = s"org.apache.flink.api.common.typeinfo.TypeInformation[$t]"

  val window = "org.apache.flink.streaming.api.windowing.windows.Window"

  val timeWindow = "org.apache.flink.streaming.api.windowing.windows.TimeWindow"

  def windowedStream(t: String, k: String, w: String) = s"org.apache.flink.streaming.api.datastream.WindowedStream[$t, $k, $w]"

  def allWindowedStream(t: String, w: String) = s"org.apache.flink.streaming.api.datastream.AllWindowedStream[$t, $w]"

  def processWindowFunction(i: String, o: String, k: String, w: String) = s"org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction[$i, $o, $k, $w]"

  def processAllWindowFunction(i: String, o: String, w: String) = s"org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction[$i, $o, $w]"

  def keySelector(i: String, k: String) = s"org.apache.flink.api.java.functions.KeySelector[$i, $k]"

  val tuple = "org.apache.flink.api.java.tuple.Tuple"
}
