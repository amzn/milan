package com.amazon.milan.flink.components

import com.amazon.milan.flink.compiler.internal.RuntimeCompiledFunction
import com.amazon.milan.flink.metrics.MetricFactory
import com.amazon.milan.program.FunctionDef
import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.flink.api.common.functions.RichFilterFunction


object FilterExpressionFilterFunction {
  /**
   * The name of the metric that counts input records.
   */
  val InputRecordCounterMetricName = "input_record_count"

  /**
   * The name of the metric that counts filtered records - records that do not pass the filter.
   */
  val FilteredRecordCounterMetricName = "filtered_record_count"

  /**
   * The name of the metric that counts unfiltered records - records that pass the filter.
   */
  val UnfilteredRecordCounterMetricName = "unfiltered_record_count"
}

import com.amazon.milan.flink.components.FilterExpressionFilterFunction._


class FilterExpressionFilterFunction[T](inputType: TypeDescriptor[_],
                                        filterExpression: FunctionDef,
                                        metricFactory: MetricFactory)
  extends RichFilterFunction[T] {

  @transient private lazy val inputRecordCounter = this.metricFactory.createCounter(this.getRuntimeContext, InputRecordCounterMetricName)
  @transient private lazy val filteredRecordCounter = this.metricFactory.createCounter(this.getRuntimeContext, FilteredRecordCounterMetricName)
  @transient private lazy val unfilteredRecordCounter = this.metricFactory.createCounter(this.getRuntimeContext, UnfilteredRecordCounterMetricName)

  private val filterFunction = new RuntimeCompiledFunction[T, Boolean](this.inputType, this.filterExpression)

  override def filter(value: T): Boolean = {
    this.inputRecordCounter.increment()

    if (this.filterFunction(value)) {
      this.unfilteredRecordCounter.increment()
      true
    }
    else {
      this.filteredRecordCounter.increment()
      false
    }
  }
}
