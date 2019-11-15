package com.amazon.milan.dataformats

class PropertyNotFoundException(propertyName: String)
  extends Exception(s"A property named '$propertyName' was not found in the class.") {
}
