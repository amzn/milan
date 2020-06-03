package com.amazon.milan.typeutil

import scala.reflect.macros.whitebox
import scala.util.matching.Regex


/**
 * Trait for macro bundles to be able to create TypeDescriptor objects.
 */
trait TypeDescriptorMacroHost extends TypeInfoHost with LiftTypeDescriptorHost {
  val c: whitebox.Context

  import c.universe._

  private val scalaStandardTypeMatcher = new Regex("^scala\\.([^\\.]*)$")
  private val scalaCollectionTypeMatcher = new Regex("^scala\\.collection\\.immutable\\.([^\\.]*)$")

  private val knownDescriptors: Map[Type, TypeDescriptor[_]] = Map(
    typeOf[Boolean] -> types.Boolean,
    typeOf[Double] -> types.Double,
    typeOf[Float] -> types.Float,
    typeOf[Int] -> types.Int,
    typeOf[Long] -> types.Long,
    typeOf[String] -> types.String,
    typeOf[java.lang.String] -> types.String
  )

  /**
   * Gets a [[TypeDescriptor]] for a type.
   *
   * @tparam T The type to get a [[TypeDescriptor]] for.
   * @return A [[TypeDescriptor]]`[`[[T]]`]` for the specified type.
   */
  def getTypeDescriptor[T: c.WeakTypeTag]: TypeDescriptor[T] = {
    val weakType = c.weakTypeOf[T]
    val hopefullyFinalType = weakType.asSeenFrom(weakType, weakType.typeSymbol)
    this.getTypeDescriptorImpl[T](hopefullyFinalType)
  }

  /**
   * Gets a [[TypeDescriptor]] for a type.
   *
   * @param ty The type to get a [[TypeDescriptor]] for.
   * @return A [[TypeDescriptor]] for the specified type.
   */
  def getTypeDescriptor(ty: c.Type): TypeDescriptor[_] = {
    this.getTypeDescriptorImpl[Any](ty)
  }

  /**
   * Gets a [[TupleTypeDescriptor]] for a tuple type with field names.
   *
   * @param fieldNames A list of expressions representing the field names corresponding to the tuple type parameters.
   * @tparam T The tuple type.
   * @return An expression that evaluates to the [[TupleTypeDescriptor]] for the named tuple.
   */
  def getNamedTupleTypeDescriptor[T <: Product : c.WeakTypeTag](fieldNames: List[c.Expr[String]]): c.Expr[TupleTypeDescriptor[T]] = {
    val tupleTypeInfo = createTypeInfo[T]

    val fields = fieldNames.zip(tupleTypeInfo.genericArguments).map {
      case (name, ty) =>
        q"new com.amazon.milan.typeutil.FieldDescriptor[${ty.ty}]($name, ${ty.toTypeDescriptor}.asInstanceOf[TypeDescriptor[${ty.ty}]])"
    }

    val tupleTypeName = s"Tuple${fields.length}"
    val genericArgs = tupleTypeInfo.genericArguments.map(_.toTypeDescriptor)

    val tree = q"new ${weakTypeOf[TupleTypeDescriptor[T]]}($tupleTypeName, $genericArgs, $fields)"

    c.Expr[TupleTypeDescriptor[T]](tree)
  }

  /**
   * Gets a [[TypeDescriptor]] for a named tuple whose fields have the specified names and types.
   */
  def getNamedTupleTypeDescriptorForFieldTypes[T <: Product : c.WeakTypeTag](fields: List[(c.Expr[String], c.Type)]): c.Expr[TupleTypeDescriptor[T]] = {
    val fieldDescriptors = fields.map {
      case (fieldName, fieldType) =>
        val fieldTypeDescriptor = this.getTypeDescriptorImpl[Any](fieldType)
        q"new com.amazon.milan.typeutil.FieldDescriptor[$fieldType]($fieldName, $fieldTypeDescriptor.asInstanceOf[com.amazon.milan.typeutil.TypeDescriptor[$fieldType]])"
    }

    val genericArgs = fields.map { case (_, fieldType) => this.getTypeDescriptorImpl[Any](fieldType) }
    val tupleTypeName = s"Tuple${fields.length}"

    val tree = q"new ${weakTypeOf[TupleTypeDescriptor[T]]}($tupleTypeName, $genericArgs, $fieldDescriptors)"

    c.Expr[TupleTypeDescriptor[T]](tree)
  }

  /**
   * Gets whether a type refers to a tuple type.
   */
  def isTuple(ty: c.Type): Boolean = {
    val fullName = ty.typeSymbol.fullName
    fullName.startsWith("scala.Tuple")
  }

  private def getTypeDescriptorImpl[T](ty: c.Type): TypeDescriptor[T] = {
    knownDescriptors.get(ty) match {
      case Some(typeDesc) =>
        typeDesc.asInstanceOf[TypeDescriptor[T]]

      case None =>
        this.generateTypeDescriptor[T](ty)
    }
  }

  private def generateTypeDescriptor[T](ty: c.Type): TypeDescriptor[T] = {
    val typeName = this.getCanonicalTypeName(ty.typeSymbol.asType)
    val fields = this.getFieldDescriptors(ty)

    val genericArgs =
      ty.typeArgs.map(genericArg => getTypeDescriptor(genericArg)) match {
        case args if args.isEmpty && TypeDescriptor.isTupleTypeName(typeName) =>
          // Sometimes we get tuple types where the Type doesn't specify the generic arguments, but we can still get them
          // from the fields.
          // This happens when the type is a stream.RecordType reference, for example.
          fields.map(_.fieldType)

        case args =>
          args
      }

    if (isTuple(ty)) {
      // We don't actually want the fields from the Tuple class.
      // Only records with named fields specified by users should have tuple type descriptors with fields.
      new TupleTypeDescriptor[T](typeName, genericArgs, List())
    }
    else if (isCollection(ty)) {
      new CollectionTypeDescriptor[T](typeName, genericArgs)
    }
    else {
      new GeneratedTypeDescriptor[T](typeName, genericArgs, fields)
    }
  }

  /**
   * Gets a list of [[FieldDescriptor]] objects for the fields of a type.
   *
   * @param ty The object type.
   * @return A list of [[FieldDescriptor]] objects for the fields.
   */
  private def getFieldDescriptors(ty: c.Type): List[FieldDescriptor[_]] = {
    def isField(symbol: Symbol): Boolean = {
      symbol.isMethod &&
        symbol.isPublic &&
        symbol.asMethod.isGetter
    }

    // Find all the public getter methods, those are the fields.
    val fields = ty.decls.filter(isField).map(_.asMethod).toList

    fields.map(m => {
      val returnType = m.returnType
      val actualReturnType = returnType.asSeenFrom(ty, ty.typeSymbol)
      val fieldTypeInfo = getTypeDescriptor(actualReturnType)
      this.createFieldDescriptor(m.name.toString, fieldTypeInfo)
    })
  }

  private def createFieldDescriptor[T](name: String, typeInfo: TypeDescriptor[T]): FieldDescriptor[T] = {
    FieldDescriptor[T](name, typeInfo)
  }

  private def getCanonicalTypeName(typeSymbol: TypeSymbol): String = {
    val fullName = typeSymbol.fullName

    this.scalaStandardTypeMatcher.findFirstMatchIn(fullName) match {
      case Some(m) => return m.group(1)
      case _ => ()
    }

    this.scalaCollectionTypeMatcher.findFirstMatchIn(fullName) match {
      case Some(m) => return m.group(1)
      case _ => ()
    }

    fullName
  }

  private def isCollection(ty: c.Type): Boolean = {
    val baseClassTypes = ty.baseClasses.filter(_.isType).map(_.asType)

    baseClassTypes.exists(_.fullName.startsWith("scala.collection.Seq")) ||
      baseClassTypes.exists(_.fullName.startsWith("java.lang.Iterable"))
  }
}
