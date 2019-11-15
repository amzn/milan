package com.amazon.milan.manage

import com.amazon.milan.SemanticVersion


class ApplicationVersionNotFoundException(message: String, applicationVersionId: String, cause: Throwable)
  extends Exception(message) {

  def this(applicationId: String, version: SemanticVersion) {
    this(s"Version '$version' of application with ID '$applicationId' does not exist.", null, null)
  }

  def this(applicationId: String, applicationVersionId: String) {
    this(s"Application '$applicationId' version ID '$applicationVersionId' does not exist.", applicationVersionId, null)
  }

  def this(applicationVersionId: String, cause: Throwable) {
    this(s"ApplicationVersion with ID '$applicationVersionId' does not exist.", applicationVersionId, cause)
  }
}
