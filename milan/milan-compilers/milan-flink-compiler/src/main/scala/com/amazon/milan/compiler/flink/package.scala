package com.amazon.milan.compiler

import org.apache.flink.api.common.typeinfo.TypeInformation

import _root_.scala.language.implicitConversions


package object flink {
  implicit def extendTypeInformation[T](typeInformation: TypeInformation[T]): TypeInformationExtensions[T] =
    new TypeInformationExtensions[T](typeInformation)
}
