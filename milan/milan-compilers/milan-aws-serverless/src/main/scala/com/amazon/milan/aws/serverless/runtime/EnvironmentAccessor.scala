package com.amazon.milan.aws.serverless.runtime


trait EnvironmentAccessor {
  def getEnv(name: String): String
}


class SystemEnvironmentAccessor extends EnvironmentAccessor {
  override def getEnv(name: String): String = {
    System.getenv(name) match {
      case value if value != null =>
        value

      case _ =>
        throw new IllegalArgumentException(s"Environment variable '$name' not found.")
    }
  }
}


class MapEnvironmentAccessor(env: Map[String, String]) extends EnvironmentAccessor {
  def this(env: (String, String)*) {
    this(Map(env: _*))
  }

  override def getEnv(name: String): String =
    this.env.get(name) match {
      case Some(value) =>
        value

      case _ =>
        throw new IllegalArgumentException(s"Environment variable '$name' not found.")
    }
}
