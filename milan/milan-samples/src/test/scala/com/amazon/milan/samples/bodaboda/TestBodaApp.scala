package com.amazon.milan.samples.bodaboda

import java.time.{Duration, Instant}

import com.amazon.milan.flink.application.FlinkApplicationConfiguration
import com.amazon.milan.flink.compiler.FlinkCompiler
import com.amazon.milan.flink.testing._
import com.amazon.milan.lang.StreamGraph
import com.amazon.milan.testing.Concurrent
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.concurrent.{Await, duration}
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

    val config = new FlinkApplicationConfiguration()

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

    val driverStatusSource = config.setMemorySource(streams.inputDriverStatus, initialDriverStatus, stopRunningWhenEmpty = false, sourceId = "driver_status_source")

    val driverLocationSource = config.setMemorySource(streams.inputDriverLocation, initialDriverLocation, stopRunningWhenEmpty = false, sourceId = "driver_location_source")

    val rideRequestSource = config.setMemorySource(streams.inputRideRequests, List(), stopRunningWhenEmpty = false, sourceId = "ride_request_source")

    val rideEventSource = config.setMemorySource(streams.inputRideEvents, List(), stopRunningWhenEmpty = false, sourceId = "ride_event_source")

    // Add a sink for the driver allocation stream that puts the items into the driver status stream.
    config.addSink(streams.outputDriverAllocations, driverStatusSource.createSink())

    val driverInfoSink = config.addMemorySink(streams.outputDrierInfo, "driver_info_sink")
    val rideInfoSink = config.addMemorySink(streams.outputRideInfo, "ride_info_sink")
    val rideWaitTimeSink = config.addMemorySink(streams.outputRideWaitTimes, "ride_wait_time_sink")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    FlinkCompiler.defaultCompiler.compile(graph, config, env)

    // If the whole test doesn't finish in 30 seconds then this will throw an exception.
    val executionFinished = env.executeAsync(30)

    // Wait for the initial data to be consumed, this can take a while since the application needs to spin up.
    Concurrent.wait(() => driverInfoSink.getRecordCount == 3, Duration.ofSeconds(10))

    // We can use this queue to poll for items being added to the ride info sink.
    val rideInfoQueue = rideInfoSink.createValueQueue
    val pollWait = Duration.ofSeconds(30)

    val requestTime = Instant.now()

    // Add a ride request, then wait for the driver to be allocated.
    val request = new RideRequest("ride_1", "user_1", requestTime, randomLocation(), randomLocation())
    rideRequestSource.add(request)

    val rideAssignedInfo = rideInfoQueue.poll(pollWait)
    assertEquals("ride_1", rideAssignedInfo.rideId)
    assertEquals(RideStatus.DRIVER_ASSIGNED, rideAssignedInfo.rideStatus)

    rideEventSource.add(new RideEvent("ride_1", requestTime.plusSeconds(10), RideEventType.START, StartEventData(request.fromLocation)))
    val rideStartedInfo = rideInfoQueue.poll(pollWait)
    assertEquals("ride_1", rideStartedInfo.rideId)
    assertEquals(RideStatus.ENROUTE, rideStartedInfo.rideStatus)
    assertEquals(request.fromLocation, rideStartedInfo.lastLocation)

    rideEventSource.add(new RideEvent("ride_1", requestTime.plusSeconds(25), RideEventType.COMPLETE, CompletedEventData(request.toLocation)))
    val rideCompletedInfo = rideInfoQueue.poll(pollWait)
    assertEquals(RideStatus.COMPLETED, rideCompletedInfo.rideStatus)
    assertEquals(request.toLocation, rideCompletedInfo.lastLocation)

    val rideWaitTimeQueue = rideWaitTimeSink.createValueQueue
    val rideWaitTime = rideWaitTimeQueue.poll(pollWait)
    assertEquals(10, rideWaitTime.waitDuration.getSeconds)

    // Tell the sources to stop when they are exhausted.
    driverStatusSource.stopWhenEmpty = true
    driverLocationSource.stopWhenEmpty = true
    rideRequestSource.stopWhenEmpty = true
    rideEventSource.stopWhenEmpty = true

    assertTrue(Await.result(executionFinished, duration.Duration.create(30, duration.SECONDS)))
  }

  private def randomLocation(): Location = {
    Location(random.nextDouble(), random.nextDouble())
  }
}
