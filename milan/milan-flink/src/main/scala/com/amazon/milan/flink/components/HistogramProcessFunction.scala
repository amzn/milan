package com.amazon.milan.flink.components

import com.amazon.milan.application.metrics.HistogramDefinition
import com.amazon.milan.flink.compiler.internal.RuntimeCompiledFunction
import com.amazon.milan.typeutil.TypeDescriptor
import com.codahale.metrics.ExponentiallyDecayingReservoir
import com.typesafe.scalalogging.Logger
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.Histogram
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory


/**
 * Process that maintains a histogram metric based on values extracted from records.
 *
 * @param streamName           The name of the stream being measured.
 * @param recordType           A [[TypeDescriptor]] describing the type of records on the stream.
 * @param histogramDefinitions Configurations for the histograms.
 */
class HistogramProcessFunction[T](streamName: String,
                                  recordType: TypeDescriptor[_],
                                  histogramDefinitions: List[HistogramDefinition[_]])
  extends ProcessFunction[T, T] {

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(this.getClass))

  @transient private lazy val histograms = this.compileHistograms()

  override def processElement(record: T,
                              context: ProcessFunction[T, T]#Context,
                              collector: Collector[T]): Unit = {
    this.logger.info(s"${this.getRuntimeContext.getTaskName} - processElement")

    this.histograms.foreach {
      case (histogram, selector) => histogram.update(selector(record))
    }
  }

  /**
   * Gets a Flink [[Histogram]] metric object.
   *
   * @param histogramDefinition Configuration for the histogram.
   * @return A Flink [[Histogram]] metric object.
   */
  private def getHistogram(histogramDefinition: HistogramDefinition[_]): Histogram = {
    getRuntimeContext
      .getMetricGroup
      .histogram(s"Histogram [${histogramDefinition.name}] for stream [${this.streamName}]",
        new DropwizardHistogramWrapper(
          new com.codahale.metrics.Histogram(new ExponentiallyDecayingReservoir(
            histogramDefinition.reservoirSize, histogramDefinition.reservoirAlpha))
        ))
  }

  private def compileHistograms(): List[(Histogram, T => Long)] = {
    this.histogramDefinitions.map(histDef => {
      val valueFunc = new RuntimeCompiledFunction[T, Long](this.recordType, histDef.valueFunction).compileFunction()
      val hist = this.getHistogram(histDef)
      (hist, valueFunc)
    })
  }
}
