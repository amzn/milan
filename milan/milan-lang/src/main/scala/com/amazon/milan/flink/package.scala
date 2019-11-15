package com.amazon.milan

import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.language.implicitConversions


package object flink {
  implicit def extendTypeInformation[T](typeInformation: TypeInformation[T]): TypeInformationExtensions[T] =
    new TypeInformationExtensions[T](typeInformation)
}
