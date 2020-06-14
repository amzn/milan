package com.amazon.milan.compiler.flink.typeutil

import com.amazon.milan.typeutil.{FieldDescriptor, TypeDescriptor}
import com.fasterxml.jackson.annotation.JsonIgnore


/**
 * A [[TypeDescriptor]] for record types that are tuples.
 * Milan-Flink uses the ArrayRecord class to store these, which has implications for code generation,
 * so it's necessary for the code generator to be able to recognize these.
 */
class TupleRecordTypeDescriptor[T](val fields: List[FieldDescriptor[_]]) extends TypeDescriptor[T] {
  override val typeName: String = "ArrayRecord"
  override val genericArguments: List[TypeDescriptor[_]] = List()

  @JsonIgnore
  override def fullName: String = "com.amazon.milan.compiler.flink.types.ArrayRecord"
}
