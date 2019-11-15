package com.amazon.milan.manage


class ApplicationInstanceDefinitionRegistration(val instanceDefinitionId: String,
                                                val applicationId: String,
                                                val applicationVersionId: String)
  extends Serializable {
}
