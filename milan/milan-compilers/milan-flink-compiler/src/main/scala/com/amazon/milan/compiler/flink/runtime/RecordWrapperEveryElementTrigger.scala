package com.amazon.milan.compiler.flink.runtime

import com.amazon.milan.compiler.flink.types.RecordWrapper
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.Window


/**
 * A Flink [[Trigger]] that fires every time an element is added to a window.
 * It does not fire for EventTime or ProcessingTime notifications.
 *
 * @tparam T The type of objects in the window.
 * @tparam W The window type.
 */
class RecordWrapperEveryElementTrigger[T >: Null, TKey >: Null <: Product, W <: Window] extends Trigger[RecordWrapper[T, TKey], W] {
  override def clear(window: W, context: Trigger.TriggerContext): Unit = ()

  override def onElement(element: RecordWrapper[T, TKey], timestamp: Long, window: W, context: Trigger.TriggerContext): TriggerResult =
    TriggerResult.FIRE

  override def onEventTime(timestamp: Long, window: W, context: Trigger.TriggerContext): TriggerResult =
    TriggerResult.CONTINUE

  override def onProcessingTime(timestamp: Long, window: W, context: Trigger.TriggerContext): TriggerResult =
    TriggerResult.FIRE
}
