package com.amazon.milan.manage


class ApplicationPackageNotFoundException(val applicationPackageId: String, cause: Throwable)
  extends Exception(s"Application package with ID '$applicationPackageId' does not exist.", cause) {

  def this(applicationPackageId: String) {
    this(applicationPackageId, null)
  }
}
