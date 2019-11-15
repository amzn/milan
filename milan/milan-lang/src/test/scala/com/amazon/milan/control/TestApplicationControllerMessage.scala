package com.amazon.milan.control

import com.amazon.milan.serialization.ScalaObjectMapper
import org.junit.Assert._
import org.junit.Test

@Test
class TestApplicationControllerMessage {
  @Test
  def test_ApplicationControllerMessageEnvelope_WithStartApplicationMessage_CanSerializeAndDeserializeViaJson(): Unit = {
    val mapper = new ScalaObjectMapper()

    val message = new StartApplicationMessage("instanceId", "packageId")
    val messageJson = mapper.writeValueAsString(message)

    val envelope = new ApplicationControllerMessageEnvelope("controllerId", MessageType.StartApplication, messageJson)
    val envelopeJson = mapper.writeValueAsString(envelope)

    val outputEnvelope = mapper.readValue[ApplicationControllerMessageEnvelope](envelopeJson, classOf[ApplicationControllerMessageEnvelope])
    val outputMessage = mapper.readValue[StartApplicationMessage](outputEnvelope.messageBodyJson, classOf[StartApplicationMessage])

    assertEquals(message.applicationInstanceId, outputMessage.applicationInstanceId)
    assertEquals(message.applicationPackageId, outputMessage.applicationPackageId)
  }
}
