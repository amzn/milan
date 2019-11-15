package com.amazon.milan.control


/**
 * Contains the state of an application controller.
 *
 * @param controllerId    The ID of the controller.
 * @param applicationInfo Information about the running applications.
 */
class ApplicationControllerState(var controllerId: String, var applicationInfo: Array[ApplicationInfo])
  extends Serializable {

  def this() {
    this("", null)
  }
}


/**
 * Contains information about a Flink application and the corresponding Milan application and package information.
 *
 * @param applicationInstanceId   The ID of the application instance that is being executed in the Flink app.
 * @param controllerApplicationId The controller-specific application ID of the running instance.
 * @param applicationPackageId    The package ID that was used for execution.
 */
class ApplicationInfo(var applicationInstanceId: String,
                      var controllerApplicationId: String,
                      var applicationPackageId: String) extends Serializable {
  def this() {
    this("", "", "")
  }
}
