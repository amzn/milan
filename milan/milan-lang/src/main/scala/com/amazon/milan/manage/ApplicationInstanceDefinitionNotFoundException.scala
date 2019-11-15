package com.amazon.milan.manage


class ApplicationInstanceDefinitionNotFoundException(message: String, val instanceDefinitionId: String, cause: Throwable)
  extends Exception(message, cause) {

  def this(applicationId: String, instanceDefinitionId: String) {
    this(s"ApplicationInstanceDefinition '$instanceDefinitionId' for application '$applicationId' does not exist.", instanceDefinitionId, null)
  }

  def this(instanceDefinitionId: String, cause: Throwable) {
    this(s"ApplicationInstanceDefinition with ID '$instanceDefinitionId' does not exist.", instanceDefinitionId, cause)
  }
}
