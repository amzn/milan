package com.amazon.milan.flink

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.scala.typeutils.OptionTypeInfo

import scala.reflect.{ClassTag, classTag}


object TypeUtil {
  private val canonicalTypeNames = Map(
    "int" -> "Int",
    "float" -> "Float",
    "double" -> "Double",
    "java.lang.Object[]" -> "Array[Any]",
    "java.lang.Object" -> "Any",
    "scala.Option" -> "Option"
  )

  /**
   * Gets the full name of a type including generic arguments.
   */
  def getTypeName(ty: TypeInformation[_]): String = {
    this.getTypeClassName(ty) + this.getGenericArgumentsListString(ty)
  }

  /**
   * Gets the name of the class of type, not including generic arguments.
   */
  def getTypeClassName(ty: TypeInformation[_]): String = {
    ty match {
      case Types.INT =>
        // The mismatch between "Int" and "Integer" can cause problems.
        // For example, if a generic type parameter has a ": Numeric" implicit argument, it will work fine for
        // Int but fail for Integer.
        "Int"

      case _ =>
        getCanonicalTypeName(ty.getTypeClass.getTypeName)
    }
  }

  /**
   * Gets the canonical name of a type as it should appear in scala code.
   *
   * @param name A type or class name.
   * @return The canonical name of the type.
   */
  def getCanonicalTypeName(name: String): String = {
    canonicalTypeNames.get(name) match {
      case Some(canonicalName) =>
        canonicalName

      case None =>
        if (name.startsWith("[L")) {
          val elementTypeName = getCanonicalTypeName(name.substring(2).stripSuffix(";"))
          "Array[" + elementTypeName + "]"
        }
        else if (name.startsWith("java.lang")) {
          name.substring(10)
        }
        else {
          // Classes contained inside static types (i.e. scala object) have a $ instead of the final
          // period in the fully-qualified type name. Sometimes this $ is good and sometimes it's bad,
          // so we just remove it and add it back later when necessary.
          name.replace('$', '.')
        }
    }
  }

  /**
   * Gets the name of the Flink tuple class with the specified number of elements.
   *
   * @param elementCount The number of tuple elements.
   * @return The tuple class name.
   */
  def getTupleClassName(elementCount: Int) =
    s"${FlinkTypeNames.tuple}$elementCount"

  /**
   * Gets the name of the Flink tuple type with the specified element types.
   *
   * @param elementTypes The names of the tuple element types.
   * @return The name of the tuple type with the specified element types.
   */
  def getTupleTypeName(elementTypes: Seq[String]): String =
    getTupleClassName(elementTypes.length) + elementTypes.mkString("[", ", ", "]")

  /**
   * Create a [[TypeInformation]] for a tuple of the specified element types.
   *
   * @param elementTypes [[TypeInformation]] values representing the element types.
   * @tparam T The type of the tuple. This should be one of the org.apache.flink.api.java.tuple.Tuple classes.
   * @return A [[TupleTypeInfo]] representing the type of a tuple with the specified elements.
   */
  def createTupleTypeInfo[T <: Tuple : ClassTag](elementTypes: TypeInformation[_]*): TupleTypeInfo[T] = {
    new TupleTypeInfo[T](classTag[T].runtimeClass.asInstanceOf[Class[T]], elementTypes: _*)
  }

  /**
   * Creates a [[TypeInformation]] for an option of the specified element type.
   * #
   *
   * @param elementType [[TypeInformation]] for the element type.
   * @tparam T The element type.
   * @return An [[OptionTypeInfo]] with the specified element type information.
   */
  def createOptionTypeInfo[T](elementType: TypeInformation[T]): OptionTypeInfo[T, Option[T]] = {
    new OptionTypeInfo[T, Option[T]](elementType)
  }

  private def getGenericArgumentsListString(ty: TypeInformation[_]): String = {
    val genericArgumentNames = ty.getTypeClass.getTypeParameters.map(_.getName)

    if (genericArgumentNames.length == 0) {
      ""
    }
    else {
      val genericArguments = ty.getGenericParameters
      val genericArgumentTypes = genericArgumentNames.map(genericArguments.get)
      genericArgumentTypes.map(this.getTypeName).mkString("[", ", ", "]")
    }
  }
}
