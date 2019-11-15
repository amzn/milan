package com.amazon.milan.control

import com.amazon.milan.serialization.ScalaObjectMapper


object ApplicationControllerMessageEnvelope {
  private val mapper = new ScalaObjectMapper()

  def wrapMessage[T <: ApplicationControllerMessage](message: T,
                                                     controllerId: String = DEFAULT_CONTROLLER_ID)
  : ApplicationControllerMessageEnvelope = {
    val json = this.mapper.writeValueAsString(message)
    new ApplicationControllerMessageEnvelope(controllerId, message.messageType, json)
  }
}


class ApplicationControllerMessageEnvelope(var controllerId: String,
                                           var messageType: String,
                                           var messageBodyJson: String)
  extends Serializable {

  def this() {
    this("", MessageType.StartApplication, "")
  }
}
