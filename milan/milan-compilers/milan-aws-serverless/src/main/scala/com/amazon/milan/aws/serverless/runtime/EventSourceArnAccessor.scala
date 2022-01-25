package com.amazon.milan.aws.serverless.runtime


trait EventSourceArnAccessor {
  def getEventSourceArn(inputName: String): String
}


object EnvironmentEventSourceArnAccessor {
  def getEventSourceArnEnvironmentVariableName(inputName: String): String =
    s"InputEventSourceArn_$inputName"
}


class EnvironmentEventSourceArnAccessor extends EventSourceArnAccessor {
  override def getEventSourceArn(inputName: String): String = {
    val varName = EnvironmentEventSourceArnAccessor.getEventSourceArnEnvironmentVariableName(inputName)

    System.getenv(varName) match {
      case value if value != null =>
        value

      case _ =>
        throw new IllegalArgumentException(s"Event source arn for input '$inputName' not found in environment ($varName).")
    }
  }
}


class MapEventSourceArnAccessor(arns: Map[String, String]) extends EventSourceArnAccessor {
  def this(arns: (String, String)*) {
    this(Map(arns: _*))
  }

  override def getEventSourceArn(inputName: String): String =
    arns.get(inputName) match {
      case Some(value) =>
        value

      case _ =>
        throw new IllegalArgumentException(s"Event source arn for input '$inputName' not found.")
    }
}
