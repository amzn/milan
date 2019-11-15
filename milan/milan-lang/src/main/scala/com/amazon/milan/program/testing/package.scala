package com.amazon.milan.program

import com.amazon.milan.typeutil.{TypeDescriptor, types}


package object testing {
  def createGroupBy[T: TypeDescriptor](expr: FunctionDef): GroupBy = {
    GroupBy(createRefFor[T], expr)
  }

  def createRef(recordType: TypeDescriptor[_]): Ref = {
    val source = Ref("_")
    source.tpe = types.stream(recordType)
    source
  }

  def createRefFor[T: TypeDescriptor]: Ref = {
    createRef(implicitly[TypeDescriptor[T]])
  }
}
