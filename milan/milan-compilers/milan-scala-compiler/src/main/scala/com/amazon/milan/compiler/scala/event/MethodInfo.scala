package com.amazon.milan.compiler.scala.event

import com.amazon.milan.compiler.scala.CodeBlock
import com.amazon.milan.typeutil.TypeDescriptor

case class MethodInfo(code: CodeBlock, returnType: TypeDescriptor[_])
