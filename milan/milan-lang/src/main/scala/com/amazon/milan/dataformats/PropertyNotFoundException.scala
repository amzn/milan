package com.amazon.milan.dataformats


/**
 * An exception that indicates that a property found in a serialized object has no corresponding property in the
 * destination type.
 *
 * @param propertyName The name of the property.
 */
class PropertyNotFoundException(propertyName: String)
  extends Exception(s"A property named '$propertyName' was not found in the class.") {
}
