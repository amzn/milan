package com.amazon.milan.samples.bodaboda

import java.time.{Duration, Instant}

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.flink.testing.{ExecutionResult, TestApplicationExecutor}
import com.amazon.milan.lang.StreamGraph
import com.amazon.milan.testing.DelayedListDataSource
import com.amazon.milan.testing.applications._
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.util.Random


@Test
class TestBodaApp {
  private val random = new Random()

  @Before
  def before(): Unit = {
    // Set this to DEBUG to get additional logging for troubleshooting.
    Configurator.setRootLevel(Level.INFO)
  }

  @After
  def after(): Unit = {
    Configurator.setRootLevel(Level.INFO)
  }

  @Test
  def test_BodaApp_LifeCycleOfOneRide(): Unit = {
    val streams = BodaApp.buildApp()

    val graph = new StreamGraph(streams.outputDriverAllocations, streams.outputRideInfo, streams.outputRideWaitTimes)

    val config = new ApplicationConfiguration()

    val initialDriverStatus = List(
      new DriverStatus("driver_1", Instant.now(), DriverStatus.AVAILABLE),
      new DriverStatus("driver_2", Instant.now(), DriverStatus.AVAILABLE),
      new DriverStatus("driver_3", Instant.now(), DriverStatus.AVAILABLE),
    )

    val initialDriverLocation = List(
      new DriverLocation("driver_1", Instant.now(), randomLocation()),
      new DriverLocation("driver_2", Instant.now(), randomLocation()),
      new DriverLocation("driver_3", Instant.now(), randomLocation())
    )

    config.setListSource(streams.inputDriverStatus, runForever = true, initialDriverStatus: _*)
    config.setListSource(streams.inputDriverLocation, runForever = true, initialDriverLocation: _*)

    val requestTime = Instant.now()
    val request = new RideRequest("ride_1", "user_1", requestTime, randomLocation(), randomLocation())

    val rideRequestSource =
      DelayedListDataSource.builder()
        .wait(Duration.ofSeconds(5))
        .add(request)
        .build(runForever = true)

    config.setSource(streams.inputRideRequests, rideRequestSource)

    val rideEventSource =
      DelayedListDataSource.builder()
        .wait(Duration.ofSeconds(10))
        .add(new RideEvent("ride_1", requestTime.plusSeconds(10), RideEventType.START, StartEventData(request.fromLocation)))
        .wait(Duration.ofSeconds(5))
        .add(new RideEvent("ride_1", requestTime.plusSeconds(25), RideEventType.COMPLETE, CompletedEventData(request.toLocation)))
        .build(runForever = true)

    config.setSource(streams.inputRideEvents, rideEventSource)

    def shouldContinue(intermediateResults: ExecutionResult): Boolean = {
      true
    }

    val results = TestApplicationExecutor.executeApplication(
      graph,
      config,
      30,
      intermediateResults => shouldContinue(intermediateResults),
      streams.outputDriverAllocations,
      streams.outputDriverInfo,
      streams.outputRideInfo,
      streams.outputRideWaitTimes)

    val rideInfoRecords = results.getRecords(streams.outputRideInfo)
    assertEquals("ride_1", rideInfoRecords.head.rideId)
    assertEquals(RideStatus.DRIVER_ASSIGNED, rideInfoRecords.head.rideStatus)

    //    // Wait for the initial data to be consumed, this can take a while since the application needs to spin up.
    //    Concurrent.wait(() => driverInfoSink.getRecordCount == 3, Duration.ofSeconds(10))
    //
    //    // We can use this queue to poll for items being added to the ride info sink.
    //    val rideInfoQueue = rideInfoSink.createValueQueue
    //    val pollWait = Duration.ofSeconds(30)
    //
    //    // Add a ride request, then wait for the driver to be allocated.
    //    rideRequestSource.add(request)
    //
    //    val rideAssignedInfo = rideInfoQueue.poll(pollWait)
    //    assertEquals("ride_1", rideAssignedInfo.rideId)
    //    assertEquals(RideStatus.DRIVER_ASSIGNED, rideAssignedInfo.rideStatus)
    //
    //    rideEventSource.add(new RideEvent("ride_1", requestTime.plusSeconds(10), RideEventType.START, StartEventData(request.fromLocation)))
    //    val rideStartedInfo = rideInfoQueue.poll(pollWait)
    //    assertEquals("ride_1", rideStartedInfo.rideId)
    //    assertEquals(RideStatus.ENROUTE, rideStartedInfo.rideStatus)
    //    assertEquals(request.fromLocation, rideStartedInfo.lastLocation)
    //
    //    rideEventSource.add(new RideEvent("ride_1", requestTime.plusSeconds(25), RideEventType.COMPLETE, CompletedEventData(request.toLocation)))
    //    val rideCompletedInfo = rideInfoQueue.poll(pollWait)
    //    assertEquals(RideStatus.COMPLETED, rideCompletedInfo.rideStatus)
    //    assertEquals(request.toLocation, rideCompletedInfo.lastLocation)
    //
    //    val rideWaitTimeQueue = rideWaitTimeSink.createValueQueue
    //    val rideWaitTime = rideWaitTimeQueue.poll(pollWait)
    //    assertEquals(10, rideWaitTime.waitDuration.getSeconds)
    //
    //    // Tell the sources to stop when they are exhausted.
    //    driverStatusSource.stopWhenEmpty = true
    //    driverLocationSource.stopWhenEmpty = true
    //    rideRequestSource.stopWhenEmpty = true
    //    rideEventSource.stopWhenEmpty = true
    //
    //    assertTrue(Await.result(executionFinished, duration.Duration.create(30, duration.SECONDS)))
  }

  private def randomLocation(): Location = {
    Location(random.nextDouble(), random.nextDouble())
  }
}
