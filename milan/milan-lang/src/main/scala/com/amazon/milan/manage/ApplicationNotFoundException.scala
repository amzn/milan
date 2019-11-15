package com.amazon.milan.manage


class ApplicationNotFoundException(val applicationId: String, cause: Throwable)
  extends Exception(s"Application with ID '$applicationId' not found.", cause) {

  def this(applicationId: String) {
    this(applicationId, null)
  }
}
