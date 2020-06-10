package com.amazon.milan.tools


object ClassHelper {
  def loadClass(className: String): Class[_] = {
    val classLoader = getClass.getClassLoader
    try {
      classLoader.loadClass(className)
    }
    catch {
      case _: ClassNotFoundException =>
        classLoader.loadClass(this.replaceLastDotWithDollar(className))
    }
  }

  private def replaceLastDotWithDollar(className: String): String = {
    className.lastIndexOf('.') match {
      case i if (i < 0) || (i == className.length - 1) =>
        className

      case i =>
        className.substring(0, i) + "$" + className.substring(i + 1)
    }
  }
}
