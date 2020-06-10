package com.amazon.milan.serialization

import scala.collection.immutable.HashSet


object DataFormatFlags extends Enumeration {
  type DataFormatFlags = Value

  /**
   * Specifies that a data reader should fail if a property is encountered in the data that does not match a property
   * of the destination type.
   */
  val FailOnUnknownProperties = Value

  val None: ValueSet = DataFormatFlags.ValueSet.empty
}


object DataFormatConfiguration {
  val default: DataFormatConfiguration = this.withFlags(DataFormatFlags.None)

  /**
   * Creates a new [[DataFormatConfiguration]] with the flags replaced with the specified flags.
   *
   * @param flags Replacement data format flags.
   * @return A new [[DataFormatConfiguration]] with the specified flags.
   */
  def withFlags(flags: DataFormatFlags.ValueSet): DataFormatConfiguration = {
    DataFormatConfiguration(flags.toHashSet)
  }

  /**
   * Creates a new [[DataFormatConfiguration]] with the flags replaced with the specified flags.
   *
   * @param flags Replacement data format flags.
   * @return A new [[DataFormatConfiguration]] with the specified flags.
   */
  def withFlags(flags: DataFormatFlags.Value*): DataFormatConfiguration = {
    DataFormatConfiguration(flags.toSet.toHashSet)
  }
}


case class DataFormatConfiguration(flags: HashSet[DataFormatFlags.Value]) {
  /**
   * Gets whether a specified [[DataFormatFlags]] flag is enabled in the configuration.
   * @param configValue A [[DataFormatFlags]] value.
   * @return True if the flag is enabled, otherwise false.
   */
  def isEnabled(configValue: DataFormatFlags.Value): Boolean =
    this.flags.contains(configValue)
}
