package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.compiler.scala.{CodeBlock, MilanScalaCompilerStringExtensions, Raw}
import com.amazon.milan.program.ConstantValue
import org.apache.commons.lang.StringEscapeUtils

import scala.collection.AbstractSeq

class TypeScriptLifter {
  implicit class Interpolator(sc: StringContext) {
    def q(subs: Any*): String = {
      val partsIterator = sc.parts.iterator
      val subsIterator = subs.iterator

      var lastPart = partsIterator.next()
      val sb = new StringBuilder()

      while (subsIterator.hasNext) {
        val (unrollNextPart, unrollSeparator, partToAppend) =
          if (lastPart.endsWith("./")) {
            (true, ",\n", lastPart.substring(0, lastPart.length - 2))
          }
          else if (lastPart.endsWith("..")) {
            (true, ", ", lastPart.substring(0, lastPart.length - 2))
          }
          else if (lastPart.endsWith("//")) {
            (true, "\n", lastPart.substring(0, lastPart.length - 2))
          }
          else {
            (false, "", lastPart)
          }

        sb.append(partToAppend)

        val lifted =
          if (unrollNextPart) {
            liftSequence(subsIterator.next(), unrollSeparator)
          }
          else {
            lift(subsIterator.next())
          }
        sb.append(lifted)

        lastPart = partsIterator.next()
      }

      sb.append(lastPart)

      sb.toString()
    }

    def qc(subs: Any*): CodeBlock = {
      val value = q(subs: _*).codeStrip
      CodeBlock(value)
    }
  }

  /**
   * Converts a string into a [[CodeBlock]].
   * No processing is done in the input string to validate that it contains correct code.
   */
  def code(s: String): CodeBlock = CodeBlock(s)

  /**
   * Lifts a sequence of objects into a delimited string of lifted objects.
   * The lifted objects are written to the string as-is; there is not guarantee that they do not contain the delimiter
   * string.
   *
   * @param o         An object that implements [[AbstractSeq]].
   * @param delimiter The delimiter between lifted objects.
   * @return A string containing the lifted objects from the sequence, separated by the delimiter.
   */
  def liftSequence(o: Any, delimiter: String): String = {
    o match {
      case t: AbstractSeq[_] => t.map(lift).mkString(delimiter)
      case _ => throw new IllegalArgumentException(s"Object of type '${o.getClass.getTypeName}' is not a sequence.")
    }
  }

  /**
   * Lifts an object.
   *
   * @param o An object.
   * @return A string containing the compile-time representation of the object.
   */
  def lift(o: Any): String = {
    o match {
      case t: Array[_] => s"[${t.map(lift).mkString(", ")}]"
      case t: List[_] => s"[${t.map(lift).mkString(", ")}]"
      case t: Map[_, _] => s"{\n${t.map { case (k, v) => q"  $k: $v" }.mkString(",\n")}\n}"
      case Raw(s) => s
      case t: String => if (t == null) "null" else "\"" + StringEscapeUtils.escapeJava(t) + "\""
      case t: Boolean => t.toString
      case t: Int => t.toString
      case t: Long => t.toString
      case t if t == null => "null"
      case ConstantValue(v, _) => this.lift(v)
      case _ => throw new IllegalArgumentException(s"Can't lift object of type '${o.getClass.getTypeName}'.")
    }
  }
}
