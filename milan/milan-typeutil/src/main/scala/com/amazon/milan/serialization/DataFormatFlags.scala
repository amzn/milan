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

  def withFlags(flags: DataFormatFlags.ValueSet): DataFormatConfiguration = {
    DataFormatConfiguration(flags.toHashSet)
  }

  def withFlags(flags: DataFormatFlags.Value*): DataFormatConfiguration = {
    DataFormatConfiguration(flags.toSet.toHashSet)
  }
}


case class DataFormatConfiguration(flags: HashSet[DataFormatFlags.Value]) {
  def isEnabled(configValue: DataFormatFlags.Value): Boolean =
    this.flags.contains(configValue)
}
