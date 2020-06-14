package com.amazon.milan.compiler.flink.metrics

import org.apache.flink.metrics.{Counter, Meter}


trait Metric {
  def increment(): Unit

  def increment(amount: Long): Unit
}


class CounterWrapper(counter: Counter) extends Metric {
  override def increment(): Unit = this.counter.inc()

  override def increment(amount: Long): Unit = this.counter.inc(amount)
}


class MeterWrapper(meter: Meter) extends Metric {
  override def increment(): Unit = this.meter.markEvent()

  override def increment(amount: Long): Unit = this.meter.markEvent(amount)
}
