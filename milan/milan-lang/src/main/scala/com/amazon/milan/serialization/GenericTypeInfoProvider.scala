package com.amazon.milan.serialization

import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyOrder}


/**
 * Trait for classes that provide generic type information for use by the [[GenericTypedJsonSerializer]] class.
 */
@JsonPropertyOrder(Array("_type", "_genericArgs"))
trait GenericTypeInfoProvider {
  /**
   * Gets the name of the type, not including any generic arguments.
   */
  @JsonProperty(value = "_type", index = 0)
  def getTypeName: String = getClass.getSimpleName

  /**
   * Gets a list of [[TypeDescriptor]] objects corresponding to the generic arguments of the type.
   */
  @JsonProperty(value = "_genericArgs", index = 1)
  def getGenericArguments: List[TypeDescriptor[_]]
}
