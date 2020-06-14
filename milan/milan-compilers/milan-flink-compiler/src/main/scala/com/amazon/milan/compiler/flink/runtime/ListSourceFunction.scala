package com.amazon.milan.compiler.flink.runtime

import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.slf4j.LoggerFactory


class ListSourceFunction[T](values: List[T], runForever: Boolean) extends SourceFunction[T] {
  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  private var running = false

  override def run(sourceContext: SourceFunction.SourceContext[T]): Unit = {
    this.running = true

    values.foreach(sourceContext.collect)

    if (this.runForever) {
      this.logger.info(s"ListSourceFunction items exhausted, awaiting cancellation.")

      while (this.running) {
        Thread.sleep(100)
      }
    }
  }

  override def cancel(): Unit = {
    this.logger.info(s"ListSourceFunction cancelled.")
    this.running = false
  }
}
