package com.amazon.milan.manage

class NoApplicationVersionsException(applicationId: String)
  extends Exception(s"Application '$applicationId' has no versions.") {
}
