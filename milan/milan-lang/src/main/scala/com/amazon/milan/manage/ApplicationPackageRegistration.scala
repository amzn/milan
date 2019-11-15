package com.amazon.milan.manage


class ApplicationPackageRegistration(val applicationPackageId: String,
                                     val applicationId: String,
                                     val instanceDefinitionId: String)
  extends Serializable {
}
