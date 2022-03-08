package com.amazon.milan.aws.serverless.runtime


/**
 * Interface for getting property values.
 */
trait PropertyAccessor {
  def getProperty(name: String): String
}


/**
 * An implementation of [[PropertyAccessor]] that gets property values from environment variables.
 */
class EnvironmentPropertyAccessor extends PropertyAccessor {
  override def getProperty(name: String): String = {
    System.getenv(name)
  }
}
