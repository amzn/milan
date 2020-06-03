package com.amazon.milan.flink.generator

import com.amazon.milan.typeutil.TypeDescriptor


case class OperatorInfo(className: ClassName,
                        outputRecordType: TypeDescriptor[_],
                        outputKeyType: TypeDescriptor[_])
