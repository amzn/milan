package com.amazon.milan.lang

object TypeUtil {
  def getTypeName(cls: Class[_]): String =
    cls.getName.replace('$', '.').stripSuffix(".")
}
