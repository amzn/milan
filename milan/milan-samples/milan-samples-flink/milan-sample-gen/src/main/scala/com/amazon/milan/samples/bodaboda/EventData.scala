package com.amazon.milan.samples.bodaboda


case class StartEventData(location: Location)


case class LocationEventData(location: Location)


case class CompletedEventData(location: Location)
