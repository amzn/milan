package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.flink.components.{EveryElementTrigger, KeyFunctionKeySelector}
import com.amazon.milan.flink.{FlinkTypeNames, RuntimeEvaluator, TypeUtil}
import com.amazon.milan.program.{GroupingExpression, SlidingWindow, TimeWindowExpression, TumblingWindow}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream._
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows, WindowAssigner}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow, Window}

import scala.reflect.ClassTag


object FlinkWindowedStreamFactory {
  val typeName: String = getClass.getTypeName.stripSuffix("$")

  case class ApplyGroupByWindowResult[T, TKey, TWindow <: Window](windowedStream: WindowedStream[T, TKey, TWindow], windowTypeName: String)

  case class ApplyTimeWindowResult[T](windowedStream: AllWindowedStream[T, TimeWindow])

  def applyGroupByWindow(groupExpr: GroupingExpression,
                         inputDataStream: SingleOutputStreamOperator[_]): ApplyGroupByWindowResult[_, _, _ <: Window] = {
    val inputTypeName = TypeUtil.getTypeName(inputDataStream.getType)

    val eval = RuntimeEvaluator.instance

    val keyTypeName = groupExpr.expr.tpe.getTypeName
    val keyTypeInformation = eval.createTypeInformation(groupExpr.expr.tpe)

    eval.evalFunction[GroupingExpression, DataStream[_], TypeInformation[_], ApplyGroupByWindowResult[_, _, _ <: Window]](
      "groupExpr",
      eval.getClassName[GroupingExpression],
      "inputDataStream",
      FlinkTypeNames.dataStream(inputTypeName),
      "keyTypeInformation",
      FlinkTypeNames.typeInformation(keyTypeName),
      s"${this.typeName}.applyGroupByWindowImpl[$inputTypeName, $keyTypeName](groupExpr, inputDataStream, keyTypeInformation)",
      groupExpr,
      inputDataStream,
      keyTypeInformation)
  }

  def applyGroupByWindowImpl[T, TKey: ClassTag](groupExpr: GroupingExpression,
                                                inputDataStream: DataStream[T],
                                                keyTypeInformation: TypeInformation[TKey]): ApplyGroupByWindowResult[T, TKey, GlobalWindow] = {
    val keySelector = new KeyFunctionKeySelector[T, TKey](groupExpr.getInputRecordType, groupExpr.expr)

    val windowedStream =
      inputDataStream
        .keyBy(keySelector, keyTypeInformation)
        .window(GlobalWindows.create().asInstanceOf[WindowAssigner[Any, GlobalWindow]])
        .trigger(new EveryElementTrigger[T, GlobalWindow])

    ApplyGroupByWindowResult(windowedStream, FlinkTypeNames.globalWindow)
  }

  def applyTimeWindow(windowExpr: TimeWindowExpression,
                      inputDataStream: SingleOutputStreamOperator[_]): ApplyTimeWindowResult[_] = {
    val inputTypeName = TypeUtil.getTypeName(inputDataStream.getType)
    val eval = RuntimeEvaluator.instance

    eval.evalFunction[TimeWindowExpression, DataStream[_], ApplyTimeWindowResult[_]](
      "windowExpr",
      eval.getClassName[TimeWindowExpression],
      "inputDataStream",
      FlinkTypeNames.singleOutputStreamOperator(inputTypeName),
      s"${this.typeName}.applyTimeWindowImpl[$inputTypeName](windowExpr, inputDataStream)",
      windowExpr,
      inputDataStream)
  }

  def applyTimeWindowImpl[T](windowExpr: TimeWindowExpression,
                             inputDataStream: SingleOutputStreamOperator[T]): ApplyTimeWindowResult[T] = {
    val eventTimeStream = this.applyEventTimeImpl(windowExpr, inputDataStream)

    val windowAssigner = windowExpr match {
      case TumblingWindow(_, _, period, offset) =>
        TumblingEventTimeWindows.of(
          period.asJava.toFlinkTime,
          offset.asJava.toFlinkTime)
          .asInstanceOf[WindowAssigner[T, TimeWindow]]

      case SlidingWindow(_, _, size, slide, offset) =>
        SlidingEventTimeWindows.of(
          size.asJava.toFlinkTime,
          slide.asJava.toFlinkTime,
          offset.asJava.toFlinkTime)
          .asInstanceOf[WindowAssigner[T, TimeWindow]]
    }

    val windowedStream =
      eventTimeStream
        .windowAll(windowAssigner)
        .trigger(new EveryElementTrigger[T, TimeWindow])

    ApplyTimeWindowResult(windowedStream)
  }

  def applyTimeWindow(windowExpr: TimeWindowExpression,
                      inputKeyedStream: KeyedStream[_, _]): ApplyGroupByWindowResult[_, _, TimeWindow] = {
    val inputTypeName = TypeUtil.getTypeName(inputKeyedStream.getType)
    val keyTypeName = TypeUtil.getTypeName(inputKeyedStream.getKeyType)

    val eval = RuntimeEvaluator.instance

    eval.evalFunction[TimeWindowExpression, KeyedStream[_, _], ApplyGroupByWindowResult[_, _, TimeWindow]](
      "windowExpr",
      eval.getClassName[TimeWindowExpression],
      "inputKeyedStream",
      FlinkTypeNames.keyedStream(inputTypeName, keyTypeName),
      s"${this.typeName}.applyTimeWindowImpl[$inputTypeName, $keyTypeName](windowExpr, inputKeyedStream)",
      windowExpr,
      inputKeyedStream)
  }

  def applyTimeWindowImpl[T, TKey](windowExpr: TimeWindowExpression,
                                   inputKeyedStream: KeyedStream[T, TKey]): ApplyGroupByWindowResult[T, TKey, TimeWindow] = {
    val windowAssigner = windowExpr match {
      case TumblingWindow(_, _, period, offset) =>
        TumblingEventTimeWindows.of(
          period.asJava.toFlinkTime,
          offset.asJava.toFlinkTime)
          .asInstanceOf[WindowAssigner[T, TimeWindow]]

      case SlidingWindow(_, _, size, slide, offset) =>
        SlidingEventTimeWindows.of(
          size.asJava.toFlinkTime,
          slide.asJava.toFlinkTime,
          offset.asJava.toFlinkTime)
          .asInstanceOf[WindowAssigner[T, TimeWindow]]
    }

    val windowedStream =
      inputKeyedStream
        .window(windowAssigner)
        .trigger(new EveryElementTrigger[T, TimeWindow])

    ApplyGroupByWindowResult(windowedStream, FlinkTypeNames.timeWindow)
  }

  def applyEventTime(windowExpr: TimeWindowExpression,
                     dataStream: SingleOutputStreamOperator[_]): SingleOutputStreamOperator[_] = {
    val inputTypeName = TypeUtil.getTypeName(dataStream.getType)
    val eval = RuntimeEvaluator.instance
    eval.evalFunction[TimeWindowExpression, SingleOutputStreamOperator[_], SingleOutputStreamOperator[_]](
      "windowExpr",
      eval.getClassName[TimeWindowExpression],
      "dataStream",
      FlinkTypeNames.singleOutputStreamOperator(inputTypeName),
      s"${this.typeName}.applyEventTimeImpl[$inputTypeName](windowExpr, dataStream)",
      windowExpr,
      dataStream)
  }

  def applyEventTimeImpl[T](windowExpr: TimeWindowExpression,
                            dataStream: SingleOutputStreamOperator[T]): SingleOutputStreamOperator[T] = {
    val timeAssigner = new TimeExtractorEventTimeAssigner[T](
      windowExpr.getInputRecordType,
      windowExpr.expr,
      windowExpr.size.asJava.multipliedBy(10))

    dataStream.assignTimestampsAndWatermarks(timeAssigner)
  }
}
