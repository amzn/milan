package com.amazon.milan.application

import com.amazon.milan.Id
import com.amazon.milan.serialization.MilanObjectMapper


/**
 * Represents an instance of an application, including the application definition and the run configuration.
 *
 * @param instanceDefinitionId The unique ID of the instance.
 * @param application          The application definition.
 * @param config               The application configuration.
 */
class ApplicationInstance(val instanceDefinitionId: String,
                          val application: Application,
                          val config: ApplicationConfiguration) {

  def this(application: Application, config: ApplicationConfiguration) {
    this(Id.newId(), application, config)
  }

  /**
   * Gets a string containing a JSON represention of the application instance.
   *
   * @return A JSON string.
   */
  def toJsonString: String = {
    MilanObjectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this)
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: ApplicationInstance =>
      this.instanceDefinitionId == o.instanceDefinitionId &&
        this.application.equals(o.application) &&
        this.config.equals(o.config)

    case _ => false
  }
}
