package com.amazon.milan.lang.internal

import com.amazon.milan.lang.{FieldStatement, TupleStream}
import com.amazon.milan.program.FieldDefinition
import com.amazon.milan.program.internal.MappedStreamHost
import com.amazon.milan.typeutil.TypeDescriptorMacroHost

import scala.reflect.macros.whitebox


trait StreamMacroHost extends TypeDescriptorMacroHost with MappedStreamHost {
  val c: whitebox.Context

  import c.universe._

  /**
   * Creates a [[TupleStream]] from one or more [[FieldStatement]] objects.
   *
   * @param fields A list of tuples of  [[FieldStatement]] and field types.
   * @tparam TOut The output tuple type.
   * @return An expression that evaluates to a [[TupleStream]] of the output type.
   */
  def mapTuple[TOut <: Product : c.WeakTypeTag](fields: List[(c.Expr[FieldDefinition], c.Type)]): c.Expr[TupleStream[TOut]] = {
    val fieldNamesAndTypes = fields.map {
      case (fieldDef, fieldType) =>
        val fieldName = c.Expr[String](q"$fieldDef.fieldName")
        (fieldName, fieldType)
    }

    val outputTypeDescriptor = getNamedTupleTypeDescriptorForFieldTypes[TOut](fieldNamesAndTypes)
    val fieldTrees = fields.map { case (expr, _) => expr }
    val nodeTree = createMappedToTupleStream[TOut](outputTypeDescriptor, fieldTrees)

    val outputTree = q"new ${weakTypeOf[TupleStream[TOut]]}($nodeTree, $outputTypeDescriptor.fields)"
    c.Expr[TupleStream[TOut]](outputTree)
  }
}
