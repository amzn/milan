package com.amazon.milan.serialization

import com.amazon.milan.typeutil.TypeDescriptor


/**
 * Trait for classes that allow setting generic type information.
 * This is called by [[GenericTypedJsonDeserializer]] immediately after deserializing an object.
 */
trait SetGenericTypeInfo {
  def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit
}
