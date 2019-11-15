package com.amazon.milan.serialization

import java.io.{InputStream, ObjectInputStream, ObjectStreamClass}

/**
 * A custom ObjectInputStream that overrides the resolveClass method to work around issues in the default ClassLoader
 * when running inside Flink.
 *
 * @param stream The input stream to read from.
 */
class ApplicationObjectInputStream(stream: InputStream) extends ObjectInputStream(stream) {
  /**
   * The ClassLoader that loaded this class. If running in Flink this will be the FlinkUserCodeClassLoader, which
   * is the one we need to load any of our Milan infrastructure classes.
   * It will not, however, correctly load some basic java classes.
   */
  private val classLoader = getClass.getClassLoader

  override def resolveClass(desc: ObjectStreamClass): Class[_] = {
    try {
      this.classLoader.loadClass(desc.getName)
    }
    catch {
      case _: ClassNotFoundException =>
        // FlinkUserCodeClassLoader will fail to load some basic java classes, such as java.lang.Integer, for some
        // reason. Falling back to the default resolveClass implementation should successfully load these classes.
        super.resolveClass(desc)
    }
  }
}
