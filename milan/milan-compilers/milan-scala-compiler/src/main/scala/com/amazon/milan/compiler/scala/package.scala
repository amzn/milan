package com.amazon.milan.compiler

import java.io.OutputStream
import java.nio.charset.StandardCharsets

import com.amazon.milan.typeutil.TypeDescriptor

import _root_.scala.language.implicitConversions


package object scala {

  implicit class MilanScalaCompilerStringExtensions(s: String) {
    def strip: String =
      this.s.stripMargin.stripLineEnd.stripPrefix("\n")

    def indent(level: Int): String = {
      val prefix = Seq.tabulate(level)(_ => "  ").mkString
      this.s.lines.map(line => prefix + line).mkString("\n")
    }

    def indentTail(level: Int): String = {
      val prefix = Seq.tabulate(level)(_ => "  ").mkString
      this.s.lines.zipWithIndex.map {
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

}
