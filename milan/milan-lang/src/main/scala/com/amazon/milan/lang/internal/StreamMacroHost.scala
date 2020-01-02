package com.amazon.milan.lang.internal

import com.amazon.milan.lang.{FieldStatement, Stream}
import com.amazon.milan.program.FieldDefinition
import com.amazon.milan.program.internal.MappedStreamHost
import com.amazon.milan.typeutil.TypeDescriptorMacroHost

import scala.reflect.macros.whitebox


trait StreamMacroHost extends TypeDescriptorMacroHost with MappedStreamHost {
  val c: whitebox.Context

  import c.universe._

  /**
   * Creates a [[Stream]] from one or more [[FieldStatement]] objects.
   *
   * @param fields A list of tuples of  [[FieldStatement]] and field types.
   * @tparam TOut The output tuple type.
   * @return An expression that evaluates to a [[Stream]] of the output type.
   */
  def mapTuple[TOut <: Product : c.WeakTypeTag](fields: List[(c.Expr[FieldDefinition], c.Type)]): c.Expr[Stream[TOut]] = {
    val fieldNamesAndTypes = fields.map {
      case (fieldDef, fieldType) =>
        val fieldName = c.Expr[String](q"$fieldDef.fieldName")
        (fieldName, fieldType)
    }

    val outputTypeDescriptor = getNamedTupleTypeDescriptorForFieldTypes[TOut](fieldNamesAndTypes)
    val fieldTrees = fields.map { case (expr, _) => expr }
    val nodeTree = createMappedToTupleStream[TOut](outputTypeDescriptor, fieldTrees)

    val outputTree = q"new ${weakTypeOf[Stream[TOut]]}($nodeTree, $outputTypeDescriptor)"
    c.Expr[Stream[TOut]](outputTree)
  }

  def warnIfNoRecordId[T: c.WeakTypeTag](): Unit = {
    val info = createTypeInfo[T]
    val recordIdFields = Set("recordId", "getRecordId")

    if (!info.fields.exists(field => recordIdFields.contains(field.name))) {
      c.warning(c.enclosingPosition, s"Lineage tracking for records of type '${info.getFullName}' will not be performed because it does not contain a record ID field.")
    }
  }
}
