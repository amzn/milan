package com.amazon.milan.typeutil


trait TypeProvider {
  /**
   * Gets whether this type provider can provide a type descriptor for a type.
   *
   * @param typeName The name of a type, not including generic argument.
   * @return True if this type provider can provide the type, otherwise false.
   */
  def canProvideType(typeName: String): Boolean

  /**
   * Gets the type descriptor for a type, or null if the type cannot be provided.
   *
   * @param typeName         The name of a type, not including generic arguments.
   * @param genericArguments [[TypeDescriptor]] objects for the generic arguments of the type.
   * @tparam T The type parameter of the type descriptor.
   * @return A [[TypeDescriptor]]`[`[[T]]`]` for the requested type name, or null.
   */
  def getTypeDescriptor[T](typeName: String, genericArguments: List[TypeDescriptor[_]]): TypeDescriptor[T]
}
