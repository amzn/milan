package com.amazon.milan.application

import com.amazon.milan.Id
import com.amazon.milan.serialization.ScalaObjectMapper


class ApplicationInstance(val instanceDefinitionId: String,
                          val application: Application,
                          val config: ApplicationConfiguration) {

  def this(application: Application, config: ApplicationConfiguration) {
    this(Id.newId(), application, config)
  }

  /**
   * Gets a string containing the JSON representing the application instance.
   *
   * @return A JSON string.
   */
  def toJsonString: String = {
    ScalaObjectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this)
  }
}
