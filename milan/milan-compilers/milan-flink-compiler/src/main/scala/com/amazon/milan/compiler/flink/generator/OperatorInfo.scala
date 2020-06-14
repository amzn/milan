package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.compiler.scala.ClassName
import com.amazon.milan.typeutil.TypeDescriptor


case class OperatorInfo(className: ClassName,
                        outputRecordType: TypeDescriptor[_],
                        outputKeyType: TypeDescriptor[_])
