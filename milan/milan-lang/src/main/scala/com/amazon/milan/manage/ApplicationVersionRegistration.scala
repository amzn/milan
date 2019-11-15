package com.amazon.milan.manage

import com.amazon.milan.SemanticVersion


/**
 * Represents a specific version of an application.
 *
 * @param applicationVersionId The unique ID of this application version.
 * @param applicationId        The ID of the application.
 * @param version              The version number.
 */
class ApplicationVersionRegistration(val applicationVersionId: String,
                                     val applicationId: String,
                                     val version: SemanticVersion)
  extends Serializable {
}
