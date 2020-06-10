package com.amazon.milan.flink.generator

import com.amazon.milan.program.{ExternalStream, Filter, FlatMap, FullJoin, GroupBy, LeftJoin, SlidingWindow, StreamExpression, StreamMap, TumblingWindow}

object StreamNaming {
  def getStreamName(streamExpr: StreamExpression): String = {
    implicit class StreamNameInterpolator(sc: StringContext) {
      def n(subs: Any*): String = {
        val partsIterator = sc.parts.iterator
        val subsIterator = subs.iterator

        val sb = new StringBuilder(partsIterator.next())
        while (subsIterator.hasNext) {
          sb.append(getStreamName(subsIterator.next().asInstanceOf[StreamExpression]))
          sb.append(partsIterator.next())
        }

        sb.toString()
      }
    }


    streamExpr match {
      case ExternalStream(_, nodeName, _) =>
        nodeName

      case FullJoin(left, right, _) =>
        n"FullJoin [$left] with [$right]"

      case LeftJoin(left, right, _) =>
        n"LeftJoin [$left] with [$right]"

      case Filter(source, _) =>
        n"Filter $source"

      case StreamMap(source, _) =>
        n"Map $source"

      case FlatMap(source, _) =>
        n"FlatMap $source"

      case GroupBy(source, _) =>
        n"Group [$source]"

      case TumblingWindow(source, _, _, _) =>
        n"TumblingWindow of [$source]"

      case SlidingWindow(source, _, _, _, _) =>
        n"SlidingWindow of [$source]"

      case _ =>
        throw new IllegalArgumentException(s"Unsupported stream expression: $streamExpr")
    }
  }

}
