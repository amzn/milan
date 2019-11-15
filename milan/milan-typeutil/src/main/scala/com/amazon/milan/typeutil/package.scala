package com.amazon.milan

import scala.collection.mutable.ListBuffer
import scala.language.experimental.macros
import scala.language.implicitConversions


package object typeutil {
  implicit def createTypeDescriptor[T]: TypeDescriptor[T] = macro TypeDescriptorMacros.create[T]

  implicit def createTypeJoiner[TLeft, TRight]: TypeJoiner[TLeft, TRight] = macro TypeJoinerMacros.createTypeJoiner[TLeft, TRight]

  /**
   * Gets the names of the type arguments from a generic type name.
   *
   * @param typeFullName The full name of a generic type.
   * @return A list of the generic argument type names.
   */
  def getGenericArgumentTypeNames(typeFullName: String): List[String] = {
    val BEGIN = 0
    val CLASS_NAME = 1
    val TYPE_ARG_LIST = 2

    var depth = 0
    var state = BEGIN
    val currentTypeName = new StringBuilder
    var typeNames = ListBuffer.empty[String]
    var listEndChars = List.empty[Char]

    try {
      typeFullName.foreach(c => {
        state match {
          case BEGIN =>
            c match {
              case '(' =>
                state = TYPE_ARG_LIST
                listEndChars = List(')')
                depth = 1

              case _ =>
                state = CLASS_NAME
            }

          case CLASS_NAME =>
            c match {
              case '[' =>
                state = TYPE_ARG_LIST
                listEndChars = List(']')
                depth = 1

              case _ =>
                ()
            }

          case TYPE_ARG_LIST if depth == 1 =>
            c match {
              case '[' =>
                depth += 1
                listEndChars = ']' :: listEndChars
                currentTypeName.append(c)

              case '(' =>
                depth += 1
                listEndChars = ')' :: listEndChars
                currentTypeName.append(c)

              case ',' =>
                typeNames += currentTypeName.toString().trim
                currentTypeName.clear()

              case _ if c == listEndChars.head =>
                depth -= 1
                typeNames += currentTypeName.toString().trim
                listEndChars = listEndChars.tail

              case _ =>
                currentTypeName.append(c)
            }

          case TYPE_ARG_LIST if depth > 1 =>
            currentTypeName.append(c)
            c match {
              case '[' =>
                depth += 1
                listEndChars = ']' :: listEndChars

              case '(' =>
                depth += 1
                listEndChars = ')' :: listEndChars

              case _ if c == listEndChars.head =>
                depth -= 1
                listEndChars = listEndChars.tail

              case _ =>
                ()
            }
        }
      })
    }
    catch {
      case ex: Exception =>
        throw new UnsupportedTypeException(s"Error parsing type name '$typeFullName': ${ex.getMessage}", ex)
    }

    typeNames.toList
  }

  /**
   * Gets the name of a type without the generic arguments.
   *
   * @param typeFullName The full name of a type.
   * @return The name of the type not including generic arguments.
   */
  def getTypeNameWithoutGenericArguments(typeFullName: String): String = {
    if (typeFullName.startsWith("(")) {
      // This is a tuple, so we need to count the generic aguments to get the class name.
      val genericArgCount = getGenericArgumentTypeNames(typeFullName).length
      s"Tuple$genericArgCount"
    }
    else {
      typeFullName.indexOf('[') match {
        case i if i > 0 => typeFullName.substring(0, i)
        case _ => typeFullName
      }
    }
  }

  implicit class TypeDescriptorExtensions[T](typeDesc: TypeDescriptor[T]) {
    def isCollection: Boolean = this.typeDesc.isInstanceOf[CollectionTypeDescriptor[_]]

    def isTuple: Boolean = this.typeDesc.isInstanceOf[TupleTypeDescriptor[_]]

    def isNumeric: Boolean = this.typeDesc.isInstanceOf[NumericTypeDescriptor[_]]

    def isStream: Boolean = this.typeDesc.isInstanceOf[StreamTypeDescriptor]

    def asStream: StreamTypeDescriptor = this.typeDesc.asInstanceOf[StreamTypeDescriptor]
  }

}
