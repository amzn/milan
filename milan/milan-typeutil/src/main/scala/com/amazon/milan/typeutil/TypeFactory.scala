package com.amazon.milan.typeutil

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


class TypeFactory(typeProviders: List[TypeProvider]) {
  private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  def this() {
    this(TypeFactory.listTypeProviders().toList)
  }

  /**
   * Gets the type descriptor for a type.
   *
   * @param typeName The name of a type.
   * @tparam T The type parameter of the type descriptor.
   * @return A [[TypeDescriptor]]`[`[[T]]`]`.
   */
  def getTypeDescriptor[T](typeName: String): TypeDescriptor[T] = {
    val genericArgs =
      if (TypeDescriptor.isGenericTypeName(typeName)) {
        val genericArgTypeNames = getGenericArgumentTypeNames(typeName)
        genericArgTypeNames.map(this.getTypeDescriptor[Any])
      }
      else {
        List()
      }

    val typeBaseName = getTypeNameWithoutGenericArguments(typeName)

    val providers = this.typeProviders.filter(_.canProvideType(typeBaseName))

    if (providers.isEmpty) {
      throw new TypeNotPresentException(typeName, null)
    }

    else if (providers.length > 1) {
      this.logger.warn(s"Type '$typeName' has more than one provider. An arbitrary provider will be used.")
    }

    providers.map(_.getTypeDescriptor[T](typeBaseName, genericArgs)).find(_ != null) match {
      case Some(typeDescriptor) =>
        typeDescriptor

      case None =>
        throw new TypeNotPresentException(typeName, null)
    }
  }
}


object TypeFactory {
  private var typeProviders: List[TypeProvider] = List(new ReflectionTypeProvider(getClass.getClassLoader))

  def overrideTypeProviders(provider: TypeProvider): Unit = {
    this.typeProviders = List(provider)
  }

  def registerTypeProvider(provider: TypeProvider): Unit = {
    this.typeProviders = this.typeProviders :+ provider
  }

  def listTypeProviders(): Iterable[TypeProvider] = {
    this.typeProviders
  }
}
