package com.amazon.milan.control.client

import com.amazon.milan.Id
import com.amazon.milan.control.{ApplicationControllerMessageEnvelope, _}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


/**
 * An application controller client that issues commands and reads state via streams.
 *
 * @param messageSink  A function where commands are sent.
 * @param getNewState  A function that returns updated controller state, or None if no new state information is available.
 * @param controllerId The controller ID.
 */
class StreamApplicationControllerClient(messageSink: ApplicationControllerMessageEnvelope => Unit,
                                        getNewState: () => Option[ApplicationControllerState],
                                        controllerId: String = DEFAULT_CONTROLLER_ID)
  extends ApplicationControllerClient {

  private var latestState: ApplicationControllerState = _

  private val logger = Logger(LoggerFactory.getLogger(getClass))

  override def startApplication(applicationPackageId: String, createDashboard: Boolean = false): String = {
    // Generate a unique ID for the new instance.
    val instanceId = Id.newId()

    val message = new StartApplicationMessage(instanceId, applicationPackageId, createDashboard)
    val envelope = ApplicationControllerMessageEnvelope.wrapMessage(message, controllerId)

    this.messageSink(envelope)

    instanceId
  }

  override def stopApplication(applicationInstanceId: String, deleteDashboard: Boolean = true): Unit = {
    val message = new StopApplicationMessage(applicationInstanceId, deleteDashboard)
    val envelope = ApplicationControllerMessageEnvelope.wrapMessage(message)
    this.messageSink(envelope)
  }

  override def snapshotApplication(applicationInstanceId: String, targetDirectory: String): Unit = {
    val message = new SnapshotApplicationMessage(applicationInstanceId, targetDirectory)
    val envelope = ApplicationControllerMessageEnvelope.wrapMessage(message)
    this.messageSink(envelope)
  }

  override def restoreApplication(applicationPackageId: String, snapshotPath: String): String = {
    // Generate a unique ID for the new instance.
    val instanceId = Id.newId()

    val message = new RestoreApplicationMessage(instanceId, applicationPackageId, snapshotPath)
    val envelope = ApplicationControllerMessageEnvelope.wrapMessage(message, controllerId)

    this.messageSink(envelope)

    instanceId
  }

  override def listRunningApplications(): Array[RunningApplicationInstance] = {
    // If the getNewState() function returns None then the state has not changed.
    this.latestState =
      this.getNewState() match {
        case None =>
          this.logger.info("Controller state hasn't changed, returning previously cached state.")
          this.latestState

        case Some(state) =>
          this.logger.info("Controller state has changed.")
          state
      }

    // If we've never seen a state record then assume there are no applications running.
    if (this.latestState == null) {
      Array()
    }
    else {
      this.latestState.applicationInfo.map(app => RunningApplicationInstance(app.applicationInstanceId, app.applicationPackageId))
    }
  }
}
