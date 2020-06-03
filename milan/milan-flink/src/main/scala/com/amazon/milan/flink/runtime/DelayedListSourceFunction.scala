package com.amazon.milan.flink.runtime

import com.amazon.milan.testing.ItemOrDelay
import org.apache.flink.streaming.api.functions.source.SourceFunction


class DelayedListSourceFunction[T](items: List[ItemOrDelay[T]],
                                   runForever: Boolean)
  extends SourceFunction[T] {

  @transient private var cancelled = false

  override def run(context: SourceFunction.SourceContext[T]): Unit = {
    def collectOrWait(item: ItemOrDelay[T]): Unit = {
      item.item match {
        case Some(record) => context.collect(record)
        case None => ()
      }

      item.delay match {
        case Some(delay) => Thread.sleep(delay.toMillis)
        case None => ()
      }
    }

    this.items
      .takeWhile(_ => !this.cancelled)
      .foreach(collectOrWait)

    if (this.runForever) {
      while (!this.cancelled) {
        Thread.sleep(100)
      }
    }
  }

  override def cancel(): Unit =
    this.cancelled = true
}
