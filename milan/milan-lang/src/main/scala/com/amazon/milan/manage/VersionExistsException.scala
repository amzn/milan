package com.amazon.milan.manage

import com.amazon.milan.SemanticVersion

class VersionExistsException(val applicationId: String, val version: SemanticVersion)
  extends Exception(s"Version '$version' of application with ID '$applicationId' already exists.") {
}
