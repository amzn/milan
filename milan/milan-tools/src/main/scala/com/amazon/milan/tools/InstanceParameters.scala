package com.amazon.milan.tools


object InstanceParameters {
  val empty: InstanceParameters = new InstanceParameters {
    /**
     * Gets all compiler parameters.
     *
     * @return A list of tuples of parameter name and parameter value. Names can be duplicated.
     */
    override def getAllValues: List[(String, String)] = List.empty
  }
}


/**
 * Interface for accessing Milan compiler parameters.
 */
trait InstanceParameters {
  /**
   * Gets all compiler parameters.
   *
   * @return A list of tuples of parameter name and parameter value. Names can be duplicated.
   */
  def getAllValues: List[(String, String)]

  /**
   * Gets the values of all parameters with the specified name.
   *
   * @param name A parameter name.
   * @return All values associated with the parameter name.
   */
  def getValues(name: String): List[String] = {
    this.getAllValues
      .filter { case (key, _) => key == name }
      .map { case (_, value) => value }
  }

  /**
   * Gets the first value specified for a parameter name.
   *
   * @param name A parameter name.
   * @return The value of the first parameter in the parameter list with the specified name.
   */
  def getValue(name: String): String = {
    this.getValueOption(name) match {
      case Some(value) =>
        value

      case None =>
        throw new IllegalArgumentException(s"No parameter named '$name' exists.")
    }
  }

  /**
   * Gets the first value specified for a parameter name, or a default value if that parameter does not exist.
   *
   * @param name         A parameter name.
   * @param defaultValue The value to return if the parameter does not exist.
   * @return The value of the first parameter in the parameter list with the specified name.
   */
  def getValueOrDefault(name: String, defaultValue: String): String = {
    this.getValueOption(name) match {
      case Some(value) => value
      case None => defaultValue
    }
  }

  /**
   * Gets the first value specified for a parameter name, or None if no parameter with that name was supplied.
   *
   * @param name A parameter name.
   * @return The value of the first parameter in the parameter list with the specified name, or None.
   */
  def getValueOption(name: String): Option[String] = {
    this.getValues(name).headOption
  }
}


class ParameterListInstanceParameters(params: List[(String, String)]) extends InstanceParameters {
  override def getAllValues: List[(String, String)] =
    this.params
}


