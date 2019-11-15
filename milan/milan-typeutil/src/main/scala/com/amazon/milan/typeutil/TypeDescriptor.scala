package com.amazon.milan.typeutil

import com.amazon.milan.serialization.{TypeInfoProvider, TypedJsonDeserializer, TypedJsonSerializer}
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import org.apache.commons.lang.builder.HashCodeBuilder

import scala.language.experimental.macros
import scala.reflect.macros.whitebox


@JsonSerialize(using = classOf[TypeDescriptorSerializer])
@JsonDeserialize(using = classOf[TypeDescriptorDeserializer])
trait TypeDescriptor[T] extends TypeInfoProvider with Serializable {
  val typeName: String
  val genericArguments: List[TypeDescriptor[_]]
  val fields: List[FieldDescriptor[_]]

  /**
   * Gets the full name of the type include generic arguments.
   */
  @JsonIgnore
  def fullName: String = {
    if (genericArguments.isEmpty) {
      typeName
    }
    else {
      typeName + genericArguments.map(_.fullName).mkString("[", ", ", "]")
    }
  }

  /**
   * Gets a verbose name of the type including generic arguments and field names for named tuple types.
   */
  @JsonIgnore
  def verboseName: String = this.fullName

  /**
   * Gets the [[FieldDescriptor]] for the field with the specified name, or None if the field does not exist.
   *
   * @param name The name of a field.
   * @return The [[FieldDescriptor]] for the field, or None.
   */
  def tryGetField(name: String): Option[FieldDescriptor[_]] = fields.find(_.name == name)

  /**
   * Gets the [[FieldDescriptor]] for the field with the specified name.
   *
   * @param name The name of a field.
   * @return The [[FieldDescriptor]] for the field.
   */
  def getField(name: String): FieldDescriptor[_] = this.tryGetField(name).get

  /**
   * Gets whether a field with the specified name exists for the type.
   */
  def fieldExists(name: String): Boolean = this.tryGetField(name).isDefined

  override def toString: String = s"""TypeDescriptor("${this.fullName}")"""

  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(obj: Any): Boolean = obj match {
    case o: TypeDescriptor[T] =>
      this.typeName.equals(o.typeName) &&
        this.genericArguments.equals(o.genericArguments) &&
        this.fields.equals(o.fields)

    case _ =>
      false
  }
}


class TypeDescriptorSerializer extends TypedJsonSerializer[TypeDescriptor[_]]


class TypeDescriptorDeserializer extends TypedJsonDeserializer[TypeDescriptor[Any]]("com.amazon.milan.typeutil")


object TypeDescriptor {
  private val knownDescriptors: Map[String, TypeDescriptor[_]] =
    Seq(types.Boolean, types.Double, types.Float, types.Int, types.Long, types.String, types.Instant, types.Duration)
      .map(ty => ty.fullName -> ty)
      .toMap

  def typeName(t: String) = s"com.amazon.milan.typeutil.TypeDescriptor[$t]"

  def of[T]: TypeDescriptor[T] = macro TypeDescriptorMacros.create[T]

  def streamOf[T]: StreamTypeDescriptor = macro TypeDescriptorMacros.createStream[T]

  def namedTupleOf[T <: Product](fieldNames: String*): TupleTypeDescriptor[T] = macro TypeDescriptorMacros.createNamedTuple[T]

  /**
   * Gets a [[TypeDescriptor]] with the specified type name and fields.
   *
   * @param typeName           The name of the type.
   * @param fieldNamesAndTypes A list of tuples of field names and associated field types.
   * @tparam T The type parameter for the type descriptor being created.
   * @return A [[TypeDescriptor]] with the specified type name and fields.
   */
  def create[T](typeName: String, fieldNamesAndTypes: List[(String, TypeDescriptor[_])]): TypeDescriptor[T] = {
    val fields = fieldNamesAndTypes.map { case (fieldName, fieldType) => new FieldDescriptor[Any](fieldName, fieldType.asInstanceOf[TypeDescriptor[Any]]) }
    this.create[T](typeName, List(), fields)
  }

  def create[T](typeName: String,
                genericArguments: List[TypeDescriptor[_]],
                fields: List[FieldDescriptor[_]]): TypeDescriptor[T] = {
    this.knownDescriptors.get(typeName) match {
      case Some(typeDesc) =>
        typeDesc.asInstanceOf[TypeDescriptor[T]]

      case _ =>
        if (TypeDescriptor.isTupleTypeName(typeName)) {
          new TupleTypeDescriptor[T](typeName, genericArguments, fields)
        }
        else {
          new ObjectTypeDescriptor[T](typeName, genericArguments, fields)
        }
    }
  }

  /**
   * Gets a [[TypeDescriptor]] using the specified type name.
   *
   * @param typeFullName The full name of the type.
   *                     If the name contains generic arguments these will be put into the generic arguments of the
   *                     returned [[TypeDescriptor]] object.
   * @tparam T The type parameter for the type descriptor being created.
   * @return A [[TypeDescriptor]] with the specified full type name.
   */
  def forTypeName[T](typeFullName: String): TypeDescriptor[T] = {
    if (isGenericTypeName(typeFullName)) {
      createGeneric[T](typeFullName)
    }
    else {
      this.create(typeFullName, List())
    }
  }

  def createNamedTuple[T](fieldNamesAndTypes: List[(String, TypeDescriptor[_])]): TupleTypeDescriptor[T] = {
    val typeName = s"Tuple${fieldNamesAndTypes.length}"
    val genericArgs = fieldNamesAndTypes.map { case (_, ty) => ty }
    val fields = fieldNamesAndTypes.map { case (name, ty) => FieldDescriptor(name, ty) }
    new TupleTypeDescriptor[T](typeName, genericArgs, fields)
  }

  def createTuple[T](tupleTypeNamePrefix: String, elementTypes: List[TypeDescriptor[_]]): TupleTypeDescriptor[T] = {
    val typeName = s"$tupleTypeNamePrefix${elementTypes.length}"
    new TupleTypeDescriptor[T](typeName, elementTypes, List())
  }

  def createTuple[T](elementTypes: List[TypeDescriptor[_]]): TupleTypeDescriptor[T] = {
    this.createTuple[T]("Tuple", elementTypes)
  }

  def iterableOf[T](elementType: TypeDescriptor[T]): TypeDescriptor[Iterable[T]] = {
    new CollectionTypeDescriptor[Iterable[T]]("Iterable", List(elementType))
  }

  def unapply(arg: TypeDescriptor[_]): Option[String] = Some(arg.fullName)

  def isTupleTypeName(name: String): Boolean = name.matches(".*Tuple\\d{1,2}$")

  def isGenericTypeName(name: String): Boolean = name.contains("[")

  private def createGeneric[T](typeFullName: String): TypeDescriptor[T] = {
    val genericArgTypeNames = getGenericArgumentTypeNames(typeFullName)
    val className = typeFullName.substring(0, typeFullName.indexOf('['))
    val genericArgs = genericArgTypeNames.map(this.forTypeName[Any])

    if (isTupleTypeName(className)) {
      new TupleTypeDescriptor[T](className, genericArgs, List())
    }
    else {
      new ObjectTypeDescriptor[T](className, genericArgs, List())
    }
  }
}


case class FieldDescriptor[T](name: String, fieldType: TypeDescriptor[T]) extends Serializable {
  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(obj: Any): Boolean = obj match {
    case o: FieldDescriptor[T] => this.name.equals(o.name) && this.fieldType.equals(o.fieldType)
  }

  def rename(newName: String): FieldDescriptor[T] = FieldDescriptor(newName, this.fieldType)
}


class TypeDescriptorMacros(val c: whitebox.Context) extends TypeDescriptorMacroHost with TypeInfoHost {

  import c.universe._

  def create[T: c.WeakTypeTag]: c.Expr[TypeDescriptor[T]] = {
    val typeDesc = this.getTypeDescriptor[T]
    c.Expr[TypeDescriptor[T]](q"$typeDesc")
  }

  def createStream[T: c.WeakTypeTag]: c.Expr[StreamTypeDescriptor] = {
    val recordTypeDesc = this.getTypeDescriptor[T]
    c.Expr[StreamTypeDescriptor](q"new ${typeOf[StreamTypeDescriptor]}($recordTypeDesc)")
  }

  def createNamedTuple[T <: Product : c.WeakTypeTag](fieldNames: c.Expr[String]*): c.Expr[TupleTypeDescriptor[T]] = {
    this.getNamedTupleTypeDescriptor[T](fieldNames.toList)
  }
}
