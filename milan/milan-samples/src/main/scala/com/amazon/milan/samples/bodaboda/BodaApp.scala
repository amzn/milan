package com.amazon.milan.samples.bodaboda

import java.time.{Duration, Instant}

import com.amazon.milan.lang._
import com.amazon.milan.serialization.ScalaObjectMapper

import scala.reflect.{ClassTag, classTag}


object BodaApp {
  def main(args: Array[String]): Unit = {
  }

  def buildApp(): BodaStreams = {
    // First define the input streams that come from users' or drivers' apps.

    // Driver status changes and driver location come in separate streams, we'll join them together to create a stream
    // of up-to-date driver information.
    val driverStatus = Stream.of[DriverStatus].withId("external_driver_status")

    val driverLocation = Stream.of[DriverLocation].withId("driver_location")

    // Ride requests from users.
    val rideRequests = Stream.of[RideRequest].withId("ride_requests")

    // Ride state events.
    val rideEvents = Stream.of[RideEvent].withId("ride_events")

    // Create a feedback loop for driver status updates that we generate later on.
    val driverStatusCycle = driverStatus.beginCycle()

    // Derive a stream that contains up-to-date information on each driver.
    // Join the status with the location to create this driver info stream.
    val driverInfo = driverStatusCycle
      .fullJoin(driverLocation)
      .where((status, loc) => status.driverId == loc.driverId && status != null && loc != null)
      .select((status, loc) => BodaApp.getDriverInfo(status, loc))
      .withId("driver_info")

    // In order to assign drivers to ride requests we'll need the latest information for each driver.
    // We'll then join that with the stream of requests to allocate a driver.
    def maxByUpdateTime(driverGroup: Stream[DriverInformation]): Stream[DriverInformation] =
      driverGroup.maxBy(info => info.updateTime)

    val latestDriverInfo =
      driverInfo
        .groupBy(info => info.driverId)
        .map((_, group) => maxByUpdateTime(group))
        .recordWindow(1)

    // There is a pretty bad race condition here, which is that we could easily allocate a driver twice.
    // The solution is probably some form of optimistic concurrency, where we allow a double allocation
    // here but detect it later and re-allocate all but one of the affected rides.
    // TODO: Decide on a mechanism to avoid the race condition that leads to double-allocation.
    val allocatedRides = rideRequests
      .leftJoin(latestDriverInfo)
      .apply((request, drivers) => BodaApp.allocateDriver(request, drivers))
      .withId("allocated_rides")

    // We need to connect this back to the input stream of driver status updates, but we can't do that within the
    // application definition because there's no support for forward references.
    // We'll have to do it using a sink that writes to the same physical stream that the status stream uses as a source.
    val driverAllocations = allocatedRides
      .map(ride => new DriverStatus(ride.driverId, Instant.now(), DriverStatus.ASSIGNED))
      .withId("driver_allocations")

    // Send the driver status changes back to the beginning so we can update the driver information.
    driverStatusCycle.closeCycle(driverAllocations)

    // Use a full join to update the ride info whenever we get a state event.
    val rideInfo = rideEvents
      .fullJoin(allocatedRides)
      .where((event, info) => event.rideId == info.rideId)
      .select((event, info) => BodaApp.updateRideInfo(info, event))
      .withId("ride_info")

    // How long it takes for rides to arrive at different locations could be interesting, let's create a stream of that.
    val rideStarts = rideEvents
      .where(event => event.eventType == RideEventType.START)
      .withId("ride_starts")

    val rideWaitTimes =
      rideRequests.fullJoin(rideStarts)
        .where((request, start) => request.rideId == start.rideId && request != null && start != null)
        .select((request, start) => BodaApp.getWaitInfo(request, start))
        .withId("ride_wait_times")

    // Ideas for extending the application:
    // * Add driver and rider ratings.
    // * Add some live stats - active requests, active drivers, active rides.
    // * Add aggregate metrics - tumbling windows of requests, active rides, completed rides, etc.
    BodaStreams(
      driverStatus,
      driverLocation,
      rideRequests,
      rideEvents,
      driverInfo,
      driverAllocations,
      rideInfo,
      rideWaitTimes)
  }

  /**
   * Gets a new [[RideInformation]] incorporating the information from a ride event.
   */
  def updateRideInfo(info: RideInformation, event: RideEvent): RideInformation = {
    if (event == null || info.updateTime.isAfter(event.eventTime)) {
      info
    }
    else {
      event.eventType match {
        case RideEventType.START =>
          val data = this.unpackEventData[StartEventData](event)
          new RideInformation(info.rideId, info.userId, info.driverId, RideStatus.ENROUTE, event.eventTime, data.location)

        case RideEventType.LOCATION =>
          val data = this.unpackEventData[LocationEventData](event)
          new RideInformation(info.rideId, info.userId, info.driverId, info.rideStatus, event.eventTime, data.location)

        case RideEventType.COMPLETE =>
          val data = this.unpackEventData[CompletedEventData](event)
          new RideInformation(info.rideId, info.userId, info.driverId, RideStatus.COMPLETED, event.eventTime, data.location)

        case RideEventType.CANCEL =>
          new RideInformation(info.rideId, info.userId, info.driverId, RideStatus.CANCELLED, event.eventTime, info.lastLocation)
      }
    }
  }

  def getDriverInfo(lastStatus: DriverStatus, lastLocation: DriverLocation): DriverInformation = {
    val updateTime = Array(lastStatus.updateTime, lastLocation.locationTime).max
    new DriverInformation(lastStatus.driverId, lastStatus, lastLocation, updateTime)
  }

  private def unpackEventData[T: ClassTag](event: RideEvent): T = {
    ScalaObjectMapper.readValue[T](event.eventData, classTag[T].runtimeClass.asInstanceOf[Class[T]])
  }

  def getWaitInfo(request: RideRequest, start: RideEvent): RideWaitInfo = {
    val waitDuration = Duration.between(request.requestTime, start.eventTime)
    new RideWaitInfo(request.rideId, request.requestTime, waitDuration, request.fromLocation)
  }

  def allocateDriver(request: RideRequest, drivers: Iterable[DriverInformation]): RideInformation = {
    // Just pick the first available driver we can find.
    val driver = drivers.filter(_.lastStatus.status == DriverStatus.AVAILABLE).head

    new RideInformation(
      request.rideId,
      request.userId,
      driver.driverId,
      RideStatus.DRIVER_ASSIGNED,
      Instant.now(),
      request.fromLocation)
  }

  case class BodaStreams(inputDriverStatus: Stream[DriverStatus],
                         inputDriverLocation: Stream[DriverLocation],
                         inputRideRequests: Stream[RideRequest],
                         inputRideEvents: Stream[RideEvent],
                         outputDriverInfo: Stream[DriverInformation],
                         outputDriverAllocations: Stream[DriverStatus],
                         outputRideInfo: Stream[RideInformation],
                         outputRideWaitTimes: Stream[RideWaitInfo])

}
