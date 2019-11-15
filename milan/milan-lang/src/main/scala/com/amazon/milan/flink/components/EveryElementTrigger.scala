package com.amazon.milan.flink.components

import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.Window


/**
 * A Flink [[Trigger]] that fires every time an element is added to a window.
 * It does not fire for EventTime or ProcessingTime notifications.
 *
 * @tparam T The type of objects in the window.
 * @tparam W The window type.
 */
class EveryElementTrigger[T, W <: Window] extends Trigger[T, W] {
  override def clear(window: W, context: Trigger.TriggerContext): Unit = ()

  override def onElement(element: T, timestamp: Long, window: W, context: Trigger.TriggerContext): TriggerResult =
    TriggerResult.FIRE

  override def onEventTime(timestamp: Long, window: W, context: Trigger.TriggerContext): TriggerResult =
    TriggerResult.CONTINUE

  override def onProcessingTime(timestamp: Long, window: W, context: Trigger.TriggerContext): TriggerResult =
    TriggerResult.FIRE
}
