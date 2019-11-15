package com.amazon.milan.program.internal

import scala.reflect.macros.whitebox


/**
 * Contains names of classes used by the Milan AST implementation.
 */
trait ProgramTypeNamesHost {
  val c: whitebox.Context

  import c.universe._

  val FieldTypeName: Tree = q"_root_.com.amazon.milan.program.Field"
  val FieldComputationExpressionTypeName: Tree = q"_root_.com.amazon.milan.program.FieldComputationExpression"
  val FilteredStreamTypeName: Tree = q"_root_.com.amazon.milan.program.FilteredStream"
  val MappedStreamTypeName: Tree = q"_root_.com.amazon.milan.program.MappedStream"
  val MapToFieldsTypeName: Tree = q"_root_.com.amazon.milan.program.MapToFields"
  val MapToRecordTypeName: Tree = q"_root_.com.amazon.milan.program.MapToRecord"
  val ProgramValidationTypeName: Tree = q"_root_.com.amazon.milan.program.ProgramValidation"
  val FunctionReferenceTypeName: Tree = q"_root_.com.amazon.milan.program.FunctionReference"
  val TypeDescriptorTypeName: Tree = q"_root_.com.amazon.milan.typeutil.TypeDescriptor[Any]"
  val FieldDescriptorTypeName: Tree = q"_root_.com.amazon.milan.typeutil.FieldDescriptor[Any]"
}
