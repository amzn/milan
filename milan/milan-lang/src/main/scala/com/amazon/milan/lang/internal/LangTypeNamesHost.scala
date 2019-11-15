package com.amazon.milan.lang.internal

import scala.reflect.macros.whitebox

/**
 * Contains names of classes used by the Milan stream language.
 */
trait LangTypeNamesHost {
  val c: whitebox.Context

  import c.universe._

  def groupedStreamTypeName(t: Type, key: Type, stream: Type): Tree = tq"_root_.com.amazon.milan.lang.GroupedStream[$t, $key, $stream]"

  def windowedStreamTypeName(t: Type, stream: Type): Tree = tq"_root_.com.amazon.milan.lang.WindowedStream[$t, $stream]"

  def joinedStreamTypeName(l: Type, r: Type): Tree = tq"_root_.com.amazon.milan.lang.JoinedStream[$l, $r]"

  def joinedStreamWithConditionTypeName(l: Type, r: Type): Tree = tq"_root_.com.amazon.milan.lang.JoinedStreamWithCondition[$l, $r]"

  def objectStreamTypeName(t: Type): Tree = tq"_root_.com.amazon.milan.lang.ObjectStream[$t]"

  def tupleStreamTypeName(t: Type): Tree = tq"_root_.com.amazon.milan.lang.TupleStream[$t]"

  val ExternalStreamNodeTypeName: Tree = q"_root_.com.amazon.milan.program.ExternalStream"
  val GroupedStreamNodeTypeName: Tree = q"_root_.com.amazon.milan.program.GroupedStream"
  val WindowedStreamNodeTypeName: Tree = q"_root_.com.amazon.milan.program.WindowedStream"
}
