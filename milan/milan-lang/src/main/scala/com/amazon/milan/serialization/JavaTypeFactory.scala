package com.amazon.milan.serialization

import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.`type`.TypeFactory
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl

import java.lang.reflect.Type


object JavaTypeFactory {
  private val knownClasses: Map[String, Class[_]] = Map(
    "Int" -> classOf[Int],
  )

  def createDefault: JavaTypeFactory =
    new JavaTypeFactory(MilanObjectMapper.getTypeFactory)
}


/**
 * Contains methods for creating [[JavaType]] and [[TypeReference]] objects for JSON deserialization, based on their
 * [[TypeDescriptor]] descriptions.
 *
 * @param typeFactory A [[TypeFactory]] that is used to construct simple types.
 */
class JavaTypeFactory(typeFactory: TypeFactory) {
  /**
   * Creates a [[JavaType]] given a [[Class]] and its generic arguments.
   *
   * @param cls              A class.
   * @param genericArguments The generic arguments of the class.
   * @return A [[JavaType]] that can be used to deserialize an instance of the type.
   */
  def makeJavaType(cls: Class[_], genericArguments: Seq[TypeDescriptor[_]]): JavaType = {
    val genericArgTypes = genericArguments.map(makeJavaType).toArray
    this.typeFactory.constructSimpleType(cls, genericArgTypes)
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

  /**
   * Creates a [[TypeReference]] from a [[TypeDescriptor]].
   *
   * @param typeDescriptor A [[TypeDescriptor]] to create a [[TypeReference]] for.
   * @return A [[TypeReference]] that refers to the type described by the [[TypeDescriptor]].
   */
  def makeTypeReference(typeDescriptor: TypeDescriptor[_]): TypeReference[_] = {
    val ty = this.createType(typeDescriptor)

    // All TypeReference does is carry around a Type instance.
    new TypeReference[Any] {
      override def getType: Type = ty
    }
  }

  private def createType(typeDescriptor: TypeDescriptor[_]): Type = {
    // The 'raw type' is the runtime class.
    // It won't contain any generic parameter information, because that gets erased.
    val rawType = this.findClass(typeDescriptor.typeName)

    // If there are no type parameters the runtime class is the Type, otherwise we need to attach the type parameters.
    if (typeDescriptor.genericArguments.isEmpty) {
      rawType
    }
    else {
      // We have type parameters so a Class[_] won't do, instead we need a ParameterizedTypeImpl.
      // The type parameters to the parameterized type are created using the generic arguments from the TypeDescriptor.
      // We can leave ownerType empty, it doesn't seem to matter.
      val genericArguments = typeDescriptor.genericArguments.map(this.createType).toArray
      ParameterizedTypeImpl.make(rawType, genericArguments, null)
    }
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
