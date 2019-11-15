package com.amazon.milan.flink.compiler

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.application.metrics.HistogramDefinition
import com.amazon.milan.flink.components.LeftJoinCoProcessFunction
import com.amazon.milan.flink.testing._
import com.amazon.milan.lang.{Stream, StreamGraph}
import com.amazon.milan.test.{IntKeyValueRecord, IntRecord}
import com.amazon.milan.testing.applications._
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.Assert._
import org.junit.Test


@Test
class TestMetrics {
  private val METRICS_REPORTER_NAME = "test_metrics"

  @Test
  def test_Metrics_HistogramMetric_CreatesMetricInReporterAndHasExpectedCount(): Unit = {
    val stream = Stream.of[IntRecord]

    val graph = new StreamGraph(stream)

    val config = new ApplicationConfiguration()
    config.setListSource(stream, IntRecord(0), IntRecord(1))
    config.addMemorySink(stream)

    val histogram = HistogramDefinition.create((r: IntRecord) => r.i.toLong, s"$METRICS_REPORTER_NAME.test_histogram")
    config.addMetric(stream, histogram)

    val flinkConfig = new Configuration()
    flinkConfig.setString(ConfigConstants.METRICS_REPORTER_PREFIX + METRICS_REPORTER_NAME + "." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, classOf[TestMetricReporter].getName)
    flinkConfig.setString(ConfigConstants.METRICS_REPORTER_PREFIX + METRICS_REPORTER_NAME + ".identifier", METRICS_REPORTER_NAME)

    val env = StreamExecutionEnvironment.createLocalEnvironment(1, flinkConfig)

    compileFromSerialized(graph, config, env)

    env.executeAtMost(10)

    val Some((_, metric)) = TestMetricReporter.histogramMetrics.find { case (key, _) => key.contains(METRICS_REPORTER_NAME) }

    assertEquals(2, metric.getCount)
    assertTrue(Array(0L, 1L).sameElements(metric.getStatistics.getValues))
  }

  @Test
  def test_Metrics_FromJoinedStream_CountsLeftInputRecordsAndOutputRecords(): Unit = {
    val left = Stream.of[IntKeyValueRecord]
    val right = Stream.of[IntKeyValueRecord]
    val joined = left.leftJoin(right)
    val output = joined.where((l, r) => l.key == r.key).select((l, r) => l).withName("join")
    val graph = new StreamGraph(output)

    val config = new ApplicationConfiguration()
    config.setMetricPrefix(METRICS_REPORTER_NAME)
    config.setListSource(left, IntKeyValueRecord(1, 1), IntKeyValueRecord(1, 2), IntKeyValueRecord(1, 3))
    config.setListSource(right, IntKeyValueRecord(1, 1), IntKeyValueRecord(1, 2), IntKeyValueRecord(1, 3))

    val flinkConfig = new Configuration()
    flinkConfig.setString(ConfigConstants.METRICS_REPORTER_PREFIX + METRICS_REPORTER_NAME + "." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, classOf[TestMetricReporter].getName)
    flinkConfig.setString(ConfigConstants.METRICS_REPORTER_PREFIX + METRICS_REPORTER_NAME + ".identifier", METRICS_REPORTER_NAME)

    val env = StreamExecutionEnvironment.createLocalEnvironment(1, flinkConfig)
    compileFromSerialized(graph, config, env)

    env.executeAtMost(10)

    val metrics = TestMetricReporter.counterMetrics.filter { case (key, _) => key.contains(METRICS_REPORTER_NAME) }
      .toList

    assertEquals(3, metrics.size)

    val leftInputRecordCounter = metrics.find { case (key, _) => key.contains(LeftJoinCoProcessFunction.LeftInputRecordsCounterMetricName) }.get._2
    assertEquals(3, leftInputRecordCounter.getCount)

    val rightInputRecordCounter = metrics.find { case (key, _) => key.contains(LeftJoinCoProcessFunction.RightInputRecordsCounterMetricName) }.get._2
    assertEquals(3, rightInputRecordCounter.getCount)

    val outputRecordCounter = metrics.find { case (key, _) => key.contains(LeftJoinCoProcessFunction.OutputRecordsCounterMetricName) }.get._2
    assertTrue(outputRecordCounter.getCount >= 3)
  }
}
