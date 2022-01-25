package com.amazon.milan.compiler

import java.io.OutputStream
import java.nio.charset.StandardCharsets
import com.amazon.milan.typeutil.TypeDescriptor

import _root_.scala.language.implicitConversions


package object scala {

  implicit class MilanScalaCompilerStringExtensions(s: String) {
    /**
     * Strips line start characters and empty lines from a string.
     */
    def codeStrip: String =
      this.s.stripMargin.stripLineEnd.stripPrefix("\n")

    def indent(level: Int): String = {
      val prefix = Seq.tabulate(level)(_ => "  ").mkString
      this.s.linesIterator.map(line => prefix + line).mkString("\n")
    }

    def indentTail(level: Int): String = {
      val prefix = Seq.tabulate(level)(_ => "  ").mkString
      this.s.linesIterator.zipWithIndex.map {
        case (line, index) =>
          if (index == 0) {
            line
          }
          else {
            prefix + line
          }
      }.mkString("\n")
    }

    def getUtf8Bytes: Array[Byte] =
      this.s.getBytes(StandardCharsets.UTF_8)
  }

  implicit class MilanScalaCompilerTypeDescriptorExtensions[_](t: TypeDescriptor[_]) {
    def toTerm: ClassName = {
      if (t.isTuple && t.genericArguments.isEmpty) {
        ClassName("Product")
      }
      else {
        ClassName(t.fullName)
      }
    }
  }

  implicit class MilanScalaOutputStreamExtensions(outputStream: OutputStream) {
    def writeUtf8(s: String): Unit = {
      this.outputStream.write(s.getUtf8Bytes)
    }
  }

  private val validIdentifierStartChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_".toSet
  private val validIdentifierChars = validIdentifierStartChars ++ "0123456789".toSet

  /**
   * Gets a valid identifier name based on the specified name.
   */
  def toValidIdentifier(name: String): String = {
    val validName = toValidName(name)

    if (this.validIdentifierStartChars.contains(validName.head)) {
      validName
    }
    else {
      "_" + validName
    }
  }

  /**
   * Converts a string to one containing only valid identifier characters.
   */
  def toValidName(name: String): String =
    name.map(c => if (!validIdentifierChars.contains(c)) '_' else c)

}
