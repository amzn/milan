package com.amazon.milan.serialization

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyOrder}


/**
 * Trait for classes that provide type information for use by the [[TypedJsonSerializer]] class.
 */
@JsonPropertyOrder(Array("_type"))
trait TypeInfoProvider {
  /**
   * Gets the name of the type.
   */
  @JsonProperty(value = "_type", index = 0)
  def getJsonTypeName: String = getClass.getSimpleName
}
