package com.amazon.milan.lang.internal

import com.amazon.milan.program.internal.MappedStreamHost
import com.amazon.milan.typeutil.TypeDescriptorMacroHost

import scala.reflect.macros.whitebox


trait StreamMacroHost extends TypeDescriptorMacroHost with MappedStreamHost {
  val c: whitebox.Context

  def warnIfNoRecordId[T: c.WeakTypeTag](): Unit = {
    val info = createTypeInfo[T]
    val recordIdFields = Set("recordId", "getRecordId")

    if (!info.isTuple && !info.fields.exists(field => recordIdFields.contains(field.name))) {
      c.warning(c.enclosingPosition, s"Lineage tracking for records of type '${info.getFullName}' will not be performed because it does not contain a record ID field.")
    }
  }
}
