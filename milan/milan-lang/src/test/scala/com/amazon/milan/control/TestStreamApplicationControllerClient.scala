package com.amazon.milan.control

import com.amazon.milan.control.client.StreamApplicationControllerClient
import org.junit.Assert._
import org.junit.Test


@Test
class TestStreamApplicationControllerClient {
  @Test
  def test_StreamApplicationControllerClient_ListApplications_ReturnsEmptyArray(): Unit = {
    def messageSink(message: ApplicationControllerMessageEnvelope): Unit = ()

    def getNewState: Option[ApplicationControllerState] = None

    val target = new StreamApplicationControllerClient(messageSink, () => getNewState)
    val runningApps = target.listRunningApplications()
    assertTrue(runningApps.isEmpty)
  }

  @Test
  def test_StreamApplicationControllerClient_StartApplication_ThenListApplications_ReturnsStartedInstanceId(): Unit = {
    def messageSink(message: ApplicationControllerMessageEnvelope): Unit = {
    }

    // We'll set this to the ID returned by the startApplication() call below.
    // It's defined here as a mutable variable so that we can return it from the getNewState() method we have
    // to pass in to the client constructor.
    var instanceId: String = ""

    val getNewState = () =>
      Some(new ApplicationControllerState(
        DEFAULT_CONTROLLER_ID,
        Array(new ApplicationInfo(instanceId, "flinkId", "packageId"))))

    val target = new StreamApplicationControllerClient(messageSink, getNewState)
    instanceId = target.startApplication("ignored")

    val runningApps = target.listRunningApplications()

    assertEquals(instanceId, runningApps(0).applicationInstanceId)
    assertEquals("packageId", runningApps(0).applicationPackageId)
  }
}
