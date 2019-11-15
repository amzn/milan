package com.amazon.milan.serialization

import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.`type`.TypeFactory


object JavaTypeFactory {
  private val knownClasses: Map[String, Class[_]] = Map(
    "Int" -> classOf[Int]
  )
}


class JavaTypeFactory(typeFactory: TypeFactory) {
  /**
   * Creates a [[JavaType]] given a [[Class]] and its generic arguments.
   *
   * @param cls              A class.
   * @param genericArguments The generic arguments of the class.
   * @return A [[JavaType]] that can be used to deserialize an instance of the type.
   */
  def makeJavaType(cls: Class[_], genericArguments: Seq[TypeDescriptor[_]]): JavaType = {
    val genericArgTypes = genericArguments.map(makeJavaType)
    this.typeFactory.constructSimpleType(cls, genericArgTypes.toArray)
  }

  /**
   * Creates a [[JavaType]] for a [[TypeDescriptor]].
   *
   * @param typeDescriptor A [[TypeDescriptor]] describing the type.
   * @return A [[JavaType]] that can be used to deserialize an instance of the type.
   */
  def makeJavaType(typeDescriptor: TypeDescriptor[_]): JavaType = {
    val cls = findClass(typeDescriptor.typeName)
    this.makeJavaType(cls, typeDescriptor.genericArguments)
  }

  private def findClass(className: String): Class[_] = {
    JavaTypeFactory.knownClasses.get(className) match {
      case Some(cls) =>
        cls

      case None =>
        val alternatives = Seq(
          className,
          this.replaceLastDotWithDollar(className),
          s"java.lang.$className",
          s"scala.$className",
          s"scala.collection.immutable.$className")

        // Return the first Class we find in the sequence of alternative class names.
        alternatives
          .map(this.tryFindClass)
          .filter(_.nonEmpty)
          .map(_.get)
          .headOption match {

          case Some(cls) =>
            cls

          case None =>
            throw new ClassNotFoundException(s"Class $className was not found.")
        }
    }
  }

  private def tryFindClass(className: String): Option[Class[_]] = {
    try {
      Some(getClass.getClassLoader.loadClass(className))
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
}
