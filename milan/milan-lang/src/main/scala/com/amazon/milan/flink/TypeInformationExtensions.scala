package com.amazon.milan.flink

import org.apache.flink.api.common.typeinfo.TypeInformation


class TypeInformationExtensions[T](typeInformation: TypeInformation[T]) {
  /**
   * Gets the full name of a type including generic arguments.
   */
  def getTypeName: String = TypeUtil.getTypeName(this.typeInformation)

  /**
   * Gets the name of the class of type, not including generic arguments.
   */
  def getTypeClassName: String = TypeUtil.getTypeClassName(this.typeInformation)
}
