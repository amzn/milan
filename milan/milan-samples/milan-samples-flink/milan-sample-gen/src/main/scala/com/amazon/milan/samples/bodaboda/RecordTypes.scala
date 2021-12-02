package com.amazon.milan.samples.bodaboda

import java.time.{Duration, Instant}

import com.amazon.milan.Id
import com.amazon.milan.serialization.MilanObjectMapper


class Location(var latitude: Double, var longitude: Double) extends Serializable {
  def this() {
    this(Double.NaN, Double.NaN)
  }

  override def toString: String = s"(${this.latitude}, ${this.longitude})"

  override def equals(obj: Any): Boolean = obj match {
    case Location(lat, lon) => this.latitude == lat && this.longitude == lon
    case _ => false
  }
}

object Location {
  def apply(latitude: Double, longitude: Double): Location = new Location(latitude, longitude)

  def unapply(arg: Location): Option[(Double, Double)] = Some((arg.latitude, arg.longitude))

  val NONE: Location = Location(Double.NaN, Double.NaN)
}


class RideRequest(var recordId: String,
                  var rideId: String,
                  var userId: String,
                  var requestTime: Instant,
                  var fromLocation: Location,
                  var toLocation: Location)
  extends Serializable {

  def this(rideId: String, userId: String, requestTime: Instant, fromLocation: Location, toLocation: Location) {
    this(Id.newId(), rideId, userId, requestTime, fromLocation, toLocation)
  }

  def this() {
    // We need a parameterless constructor so that Flink treats this as a POJO type, which has performance benefits.
    this("", "", Instant.MIN, Location.NONE, Location.NONE)
  }
}


class RideEvent(var recordId: String,
                var rideId: String,
                var eventTime: Instant,
                var eventType: String,
                var eventData: String)
  extends Serializable {

  def this(rideId: String, eventTime: Instant, eventType: String, eventData: String) {
    this(Id.newId(), rideId, eventTime, eventType, eventData)
  }

  def this(rideId: String, eventTime: Instant, eventType: String, eventData: AnyRef) {
    this(rideId, eventTime, eventType, MilanObjectMapper.writeValueAsString(eventData))
  }

  def this() {
    // We need a parameterless constructor so that Flink treats this as a POJO type, which has performance benefits.
    this("", Instant.MIN, "", "")
  }
}

object RideEventType {
  val START: String = "START"
  val COMPLETE: String = "COMPLETE"
  val CANCEL: String = "CANCEL"
  val LOCATION: String = "LOCATION"
}


class RideInformation(var recordId: String,
                      var rideId: String,
                      var userId: String,
                      var driverId: String,
                      var rideStatus: String,
                      var updateTime: Instant,
                      var lastLocation: Location)
  extends Serializable {

  def this(rideId: String, userId: String, driverId: String, rideStatus: String, updateTime: Instant, lastLocation: Location) {
    this(Id.newId(), rideId, userId, driverId, rideStatus, updateTime, lastLocation)
  }

  def this() {
    // We need a parameterless constructor so that Flink treats this as a POJO type, which has performance benefits.
    this("", "", "", "", Instant.MIN, Location.NONE)
  }
}

object RideStatus {
  val DRIVER_ASSIGNED: String = "DRIVER_ASSIGNED"
  val ENROUTE: String = "ENROUTE"
  val COMPLETED: String = "COMPLETED"
  val CANCELLED: String = "CANCELLED"
}


class RideWaitInfo(var recordId: String,
                   var rideId: String,
                   var requestTime: Instant,
                   var waitDuration: Duration,
                   var location: Location)
  extends Serializable {

  def this(rideId: String, requestTime: Instant, waitDuration: Duration, location: Location) {
    this(Id.newId(), rideId, requestTime, waitDuration, location)
  }

  def this() {
    // We need a parameterless constructor so that Flink treats this as a POJO type, which has performance benefits.
    this("", Instant.MIN, Duration.ZERO, Location.NONE)
  }
}


class DriverStatus(var recordId: String,
                   var driverId: String,
                   var updateTime: Instant,
                   var status: String)
  extends Serializable {

  def this(driverId: String, statusTime: Instant, driverStatus: String) {
    this(Id.newId(), driverId, statusTime, driverStatus)
  }

  def this() {
    // We need a parameterless constructor so that Flink treats this as a POJO type, which has performance benefits.
    this("", Instant.MIN, "")
  }
}


class DriverLocation(var recordId: String,
                     var driverId: String,
                     var locationTime: Instant,
                     var location: Location)
  extends Serializable {

  def this(driverId: String, locationTime: Instant, location: Location) {
    this(Id.newId(), driverId, locationTime, location)
  }

  def this() {
    // We need a parameterless constructor so that Flink treats this as a POJO type, which has performance benefits.
    this("", Instant.MIN, Location.NONE)
  }
}


object DriverStatus {
  val AVAILABLE: String = "AVAILABLE"
  val ASSIGNED: String = "ASSIGNED"
  val OFFLINE: String = "OFFLINE"
}


class DriverInformation(var recordId: String,
                        var driverId: String,
                        var lastStatus: DriverStatus,
                        var lastLocation: DriverLocation,
                        var updateTime: Instant)
  extends Serializable {

  def this(driverId: String,
           lastStatus: DriverStatus,
           lastLocation: DriverLocation,
           updateTime: Instant) {
    this(Id.newId(), driverId, lastStatus, lastLocation, updateTime)
  }

  def this() {
    // We need a parameterless constructor so that Flink treats this as a POJO type, which has performance benefits.
    this("", new DriverStatus(), new DriverLocation(), Instant.MIN)
  }
}
