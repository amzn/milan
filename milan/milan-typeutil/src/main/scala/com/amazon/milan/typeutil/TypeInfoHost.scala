package com.amazon.milan.typeutil

import scala.reflect.macros.whitebox

/**
 * Trait for enabling macro bundles to create TypeInfo and FieldInfo objects.
 */
trait TypeInfoHost {
  val c: whitebox.Context

  import c.universe._

  /**
   * Contains information about a field.
   *
   * @param name     The name of the field.
   * @param typeInfo A [[TypeInfo]] describing the type of the field.
   * @tparam T The type of the field.
   */
  case class FieldInfo[T](name: String, typeInfo: TypeInfo[T]) {
    def toFieldDescriptor: FieldDescriptor[T] = new FieldDescriptor[T](this.name, this.typeInfo.toTypeDescriptor)
  }

  /**
   * Contains a description of a type.
   *
   * @param ty               The Type object from the macro context representing this type.
   * @param genericArguments A list of [[TypeInfo]] objects corresponding to the generic type arguments of this type.
   * @param fields           A list of [[FieldInfo]] describing the fields of the type.
   * @tparam T The type.
   */
  case class TypeInfo[T](ty: c.Type, genericArguments: List[TypeInfo[_]], fields: List[FieldInfo[_]]) {
    /**
     * Gets a field by name.
     *
     * @param name The name of the field to get.
     * @return The [[FieldInfo]] object for the field, or None if the field does not exist for the type.
     */
    def getField(name: String): Option[FieldInfo[_]] = this.fields.find(_.name == name)

    /**
     * Gets a field by index.
     * This should only be used for Tuple types.
     * For standard object types the field ordering is not guaranteed.
     *
     * @param index The index of the field.
     * @return The [[FieldInfo]] for the field at the index.
     */
    def getField(index: Int): FieldInfo[_] = this.fields(index)

    /**
     * Gets a [[TypeName]] object referring to this type.
     *
     * @return A [[TypeName]] for this type.
     */
    def toTypeName: TypeName = TypeName(getFullName)

    /**
     * Gets a [[TypeDescriptor]]`[`[[T]]`]` representing this type.
     *
     * @return A [[TypeDescriptor]]`[`[[T]]`]` representing this type.
     */
    def toTypeDescriptor: TypeDescriptor[T] = {
      val name = this.getFullyQualifiedName
      val genericArgs = this.genericArguments.map(_.toTypeDescriptor)
      val fields = this.fields.map(_.toFieldDescriptor)
      TypeDescriptor.create[T](name, genericArgs, fields)
    }

    /**
     * Gets a [[StreamTypeDescriptor]] representing a stream of records of this type.
     *
     * @return A [[StreamTypeDescriptor]] representing a stream of records of this type.
     */
    def toStreamTypeDescriptor: StreamTypeDescriptor = {
      val recordType = this.toTypeDescriptor
      new StreamTypeDescriptor(recordType)
    }

    /**
     * Gets the full name of the type, including generic type arguments.
     * Built-in types will not have their packages included (e.g. scala.Int will return Int).
     *
     * @return The full name of the type.
     */
    def getFullName: String = {
      if (this.genericArguments.isEmpty) {
        this.getFullyQualifiedName
      }
      else {
        this.getFullyQualifiedName + this.genericArguments.map(_.getFullName).mkString("[", ", ", "]")
      }
    }

    /**
     * Gets the fully qualified name of the type, without the generic type arguments.
     * Built-in types will not have their packages included (e.g. scala.Int will return Int).
     *
     * @return The fully qualified name of the type.
     */
    private def getFullyQualifiedName: String = {
      val actualType = this.ty.asSeenFrom(this.ty, this.ty.typeSymbol)
      val name = actualType.typeSymbol.fullName

      // Built-in types are in the root scala or java packages.
      // We don't want to include the package name for built-in types.
      if (name.startsWith("scala.") || name.startsWith("java.lang.")) {
        name.split('.').last
      }
      else {
        name
      }
    }
  }

  /**
   * Encapsulates an expression tree and its output type.
   *
   * @param tree An expression tree.
   * @param ty   The type returned when the root of the tree is evaluated.
   */
  case class TypedTree(tree: c.universe.Tree, ty: c.Type)

  /**
   * Creates a [[TypeInfo]] for a type.
   *
   * @param ty The Type object from the macro context.
   * @tparam T The type.
   * @return A [[TypeInfo]] for the type.
   */
  def createTypeInfo[T](ty: c.Type): TypeInfo[T] = {
    val fieldInfo = getFieldInfo(ty)
    val genericArguments = getGenericArguments(ty)
    new TypeInfo[T](ty, genericArguments, fieldInfo)
  }

  /**
   * Creates a [[TypeInfo]] for a type where a WeakTypeTag is available.
   *
   * @tparam T The type.
   * @return A [[TypeInfo]] for the type.
   */
  def createTypeInfo[T: c.WeakTypeTag]: TypeInfo[T] =
    createTypeInfo[T](c.weakTypeOf[T])

  /**
   * Creates a [[TypeInfo]] for a Tuple type where the fields are named.
   *
   * @param ty     The Type object from the macro context.
   * @param fields A list of tuples of field name and field type.
   * @tparam T The type of the tuple.
   * @return A [[TypeInfo]] describing the tuple type.
   */
  def createTupleTypeInfo[T <: Product](ty: c.Type, fields: List[(String, c.Type)]): TypeInfo[T] = {
    val fieldInfo = getTupleFieldInfo(ty, fields)
    val genericArguments = getGenericArguments(ty)
    new TypeInfo[T](ty, genericArguments, fieldInfo)
  }

  /**
   * Gets an expression tree that evaluates to a list of [[FieldInfo]] objects corresponding to the fields of
   * a type
   *
   * @param weakType The object type.
   * @return An expression tree that evaluates to [[FieldInfo]] objects for the fields.
   */
  private def getFieldInfo(weakType: c.Type): List[FieldInfo[_]] = {
    def isField(symbol: Symbol): Boolean = {
      symbol.isMethod &&
        symbol.isPublic &&
        symbol.asMethod.isGetter
    }

    // Find all the public getter methods, those are the fields.
    val ty = weakType.asSeenFrom(weakType, weakType.typeSymbol)
    val fields = ty.decls.filter(isField).map(_.asMethod).toList

    fields.map(m => {
      val returnType = m.returnType
      val actualReturnType = returnType.asSeenFrom(ty, ty.typeSymbol)
      val fieldTypeInfo = createTypeInfo[Any](actualReturnType)
      new FieldInfo[Any](m.name.toString, fieldTypeInfo)
    })
  }

  /**
   * Gets a list of [[FieldInfo]] objects for the fields of a tuple type.
   *
   * @param ty     The Type object from the macro context for the tuple type.
   * @param fields A list of tuples of field name and field name.
   * @return A list of [[FieldInfo]] objects for the tuple fields.
   */
  private def getTupleFieldInfo(ty: c.Type, fields: List[(String, c.Type)]): List[FieldInfo[_]] = {
    fields.map {
      case (fieldName, fieldType) =>
        val actualType = fieldType.asSeenFrom(ty, ty.typeSymbol)
        val fieldTypeInfo = createTypeInfo[Any](actualType)
        new FieldInfo[Any](fieldName, fieldTypeInfo)
    }
  }

  /**
   * Gets an expression tree that evaluates to a list of TypeInformation objects corresponding the the generic
   * arguments of a type.
   *
   * @param weakType The type.
   * @return An expression tree that evaluates to a list of TypeInformation for the generic arguments.
   */
  private def getGenericArguments(weakType: c.Type): List[TypeInfo[_]] = {
    val ty = weakType.asSeenFrom(weakType, weakType.typeSymbol)
    ty.typeArgs.map(ty => createTypeInfo[Any](ty))
  }
}
