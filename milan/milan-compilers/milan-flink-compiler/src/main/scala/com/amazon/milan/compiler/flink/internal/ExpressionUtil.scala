package com.amazon.milan.compiler.flink.internal

import com.amazon.milan.program.{ExternalStream, Filter, FlatMap, GroupingExpression, JoinExpression, StreamExpression, StreamMap, Tree}


object ExpressionUtil {
  def getInputDataStreams(streamExpr: StreamExpression): List[StreamExpression] = {
    streamExpr.getChildren.flatMap(this.findDataStreams).toList
  }

  private def findDataStreams(tree: Tree): Seq[StreamExpression] = {
    tree match {
      case m: FlatMap => Seq(m)
      case m: StreamMap => Seq(m)
      case e: ExternalStream => Seq(e)
      case Filter(source, _) => this.findDataStreams(source)
      case JoinExpression(left, right, _) => findDataStreams(left) ++ findDataStreams(right)
      case GroupingExpression(source, _) => findDataStreams(source)
      case _ => Seq.empty
    }
  }
}
