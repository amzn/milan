package com.amazon.milan.flink.control

object ApplicationControllerDiagnostic {
  val DEBUG = "DEBUG"
  val INFO = "INFO"
  val WARNING = "WARN"
  val ERROR = "ERROR"
}


class ApplicationControllerDiagnostic(var level: String, var message: String) extends Serializable {
  def this() {
    this("", "")
  }
}
