package com.amazon.milan.manage


class ApplicationInstanceRegistration(val applicationInstanceId: String,
                                      val applicationId: String,
                                      val instanceDefinitionId: String,
                                      val packageId: String)
  extends Serializable {

  override def toString: String =
    s"{applicationInstanceId=$applicationInstanceId, applicationId=$applicationId, instanceDefinitionId=$instanceDefinitionId, packageId=$packageId}"
}
