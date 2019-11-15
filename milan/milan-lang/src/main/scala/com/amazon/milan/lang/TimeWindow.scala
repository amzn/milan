package com.amazon.milan.lang

import java.time._


object TimeWindow {
  /**
   * Gets a [[Duration]] that aligns the start of a time window to the specified day of the week.
   *
   * @param dayOfWeek The day of the week that should be the start of a window.
   * @return A [[Duration]] that aligns the start of a window to the specified day of the week.
   */
  def offsetWithWeekStartingOn(dayOfWeek: DayOfWeek): Duration = {
    val epochStartDayOfWeek = LocalDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC")).getDayOfWeek
    val offsetDays = (dayOfWeek.getValue - epochStartDayOfWeek.getValue) % 7
    Duration.ofDays(offsetDays)
  }
}
