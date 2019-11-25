package com.amazon.milan.typeutil

import java.time.{Duration, Instant}

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


class ReflectionTypeProvider(classLoader: ClassLoader) extends TypeProvider {
  private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  private val knownTypesByClass = Map[Class[_], TypeDescriptor[_]](
    classOf[Int] -> types.Int,
    classOf[Long] -> types.Long,
    classOf[Double] -> types.Double,
    classOf[Float] -> types.Float,
    classOf[Boolean] -> types.Boolean,
    classOf[Instant] -> types.Instant,
    classOf[Duration] -> types.Duration,
    classOf[String] -> types.String,
    classOf[Nothing] -> types.Nothing
  )

  private val knownTypesByName = Map[String, TypeDescriptor[_]](this.knownTypesByClass.values.map(td => td.typeName -> td).toList: _*)

  /**
   * Gets whether this type provider can provide a type descriptor for a type.
   *
   * @param typeName The name of a type.
   * @return True if this type provider can provide the type, otherwise false.
   */
  override def canProvideType(typeName: String): Boolean = {
    try {
      this.getTypeDescriptor[Any](typeName, List()) != null
    }
    catch {
      case _: Exception =>
        false
    }
  }

  /**
   * Gets the type descriptor for a type, or null if the type cannot be provided.
   *
   * @param typeName The name of a type.
   * @tparam T The type parameter of the type descriptor.
   * @return A [[TypeDescriptor]]`[`[[T]]`]` for the requested type name, or null.
   */
  override def getTypeDescriptor[T](typeName: String, genericArguments: List[TypeDescriptor[_]]): TypeDescriptor[T] = {
    this.knownTypesByName.get(typeName) match {
      case Some(typeDesc) =>
        typeDesc.asInstanceOf[TypeDescriptor[T]]

      case None =>
        val alternatives = Seq(
          typeName,
          this.replaceLastDotWithDollar(typeName),
          s"scala.${typeName}")

        // Return the first Class we find in the sequence of alternative class names.
        alternatives
          .map(this.tryFindClass)
          .filter(_.nonEmpty)
          .map(_.get)
          .headOption match {

          case Some(cls) =>
            this.generateTypeDescriptor[T](typeName, cls, genericArguments)

          case None =>
            this.logger.error(s"Couldn't generate TypeDescriptor for type '$typeName'.")
            null
        }
    }
  }

  private def tryFindClass(className: String): Option[Class[_]] = {
    try {
      Some(this.classLoader.loadClass(className))
    }
    catch {
      case _: ClassNotFoundException =>
        None
    }
  }

  private def replaceLastDotWithDollar(className: String): String = {
    className.lastIndexOf('.') match {
      case i if (i < 0) || (i == className.length - 1) =>
        className

      case i =>
        className.substring(0, i) + "$" + className.substring(i + 1)
    }
  }

  private def generateTypeDescriptor[T](typeName: String, cls: Class[_], genericArguments: List[TypeDescriptor[_]]): TypeDescriptor[T] = {
    this.logger.debug(s"Generating type descriptor for '$typeName'.")

    if (TypeDescriptor.isTupleTypeName(typeName)) {
      new TupleTypeDescriptor[T](typeName, genericArguments, List())
    }
    else {
      val fieldFields = cls.getDeclaredFields
        .map(field => FieldDescriptor(field.getName, this.getTypeDescriptor(field.getType, List())))
        .toList

      new ObjectTypeDescriptor[T](typeName, genericArguments, fieldFields)
    }
  }

  private def getTypeDescriptor(cls: Class[_], genericArguments: List[TypeDescriptor[_]]): TypeDescriptor[_] = {
    this.knownTypesByClass.get(cls) match {
      case Some(typeDesc) =>
        typeDesc

      case None =>
        this.generateTypeDescriptor[Any](cls.getCanonicalName, cls, genericArguments)
    }
  }
}
