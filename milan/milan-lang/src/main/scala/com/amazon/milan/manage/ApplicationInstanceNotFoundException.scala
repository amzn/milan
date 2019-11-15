package com.amazon.milan.manage


class ApplicationInstanceNotFoundException(applicationInstanceId: String, cause: Throwable)
  extends Exception(s"Application instance with ID '$applicationInstanceId' does not exist.") {
}
