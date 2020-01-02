package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.flink.components.{EveryElementTrigger, KeyFunctionKeySelector}
import com.amazon.milan.flink.{FlinkTypeNames, RuntimeEvaluator, TypeUtil}
import com.amazon.milan.program.{GroupBy, SlidingWindow, TimeWindowExpression, TumblingWindow}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream._
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows, WindowAssigner}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow, Window}

import scala.reflect.ClassTag


/**
 * Methods for applying grouping operations (GroupBy, SlidingWindow, TumblingWindow) to Flink data streams.
 */
object FlinkWindowedStreamFactory {
  val typeName: String = getClass.getTypeName.stripSuffix("$")

  trait ApplyWindowResult

  case class ApplyKeyedWindowResult[T, TKey, TWindow <: Window](windowedStream: WindowedStream[T, TKey, TWindow],
                                                                windowTypeName: String) extends ApplyWindowResult

  case class ApplyUnkeyedWindowResult[T](windowedStream: AllWindowedStream[T, TimeWindow]) extends ApplyWindowResult

  def applyGroupByWindow(groupExpr: GroupBy,
                         inputDataStream: SingleOutputStreamOperator[_]): ApplyKeyedWindowResult[_, _, _ <: Window] = {
    val inputTypeName = TypeUtil.getTypeName(inputDataStream.getType)

    val eval = RuntimeEvaluator.instance

    val keyTypeName = groupExpr.expr.tpe.getTypeName
    val keyTypeInformation = eval.createTypeInformation(groupExpr.expr.tpe)

    eval.evalFunction[GroupBy, DataStream[_], TypeInformation[_], ApplyKeyedWindowResult[_, _, _ <: Window]](
      "groupExpr",
      eval.getClassName[GroupBy],
      "inputDataStream",
      FlinkTypeNames.dataStream(inputTypeName),
      "keyTypeInformation",
      FlinkTypeNames.typeInformation(keyTypeName),
      s"${this.typeName}.applyGroupByWindowImpl[$inputTypeName, $keyTypeName](groupExpr, inputDataStream, keyTypeInformation)",
      groupExpr,
      inputDataStream,
      keyTypeInformation)
  }

  def applyGroupByWindowImpl[T, TKey: ClassTag](groupExpr: GroupBy,
                                                inputDataStream: DataStream[T],
                                                keyTypeInformation: TypeInformation[TKey]): ApplyKeyedWindowResult[T, TKey, GlobalWindow] = {
    val keySelector = new KeyFunctionKeySelector[T, TKey](groupExpr.getInputRecordType, groupExpr.expr)

    val windowedStream =
      inputDataStream
        .keyBy(keySelector, keyTypeInformation)
        .window(GlobalWindows.create().asInstanceOf[WindowAssigner[Any, GlobalWindow]])
        .trigger(new EveryElementTrigger[T, GlobalWindow])

    ApplyKeyedWindowResult(windowedStream, FlinkTypeNames.globalWindow)
  }

  def applyTimeWindow(windowExpr: TimeWindowExpression,
                      inputDataStream: SingleOutputStreamOperator[_]): ApplyUnkeyedWindowResult[_] = {
    val inputTypeName = TypeUtil.getTypeName(inputDataStream.getType)
    val eval = RuntimeEvaluator.instance

    eval.evalFunction[TimeWindowExpression, DataStream[_], ApplyUnkeyedWindowResult[_]](
      "windowExpr",
      eval.getClassName[TimeWindowExpression],
      "inputDataStream",
      FlinkTypeNames.singleOutputStreamOperator(inputTypeName),
      s"${this.typeName}.applyTimeWindowImpl[$inputTypeName](windowExpr, inputDataStream)",
      windowExpr,
      inputDataStream)
  }

  def applyTimeWindowImpl[T](windowExpr: TimeWindowExpression,
                             inputDataStream: SingleOutputStreamOperator[T]): ApplyUnkeyedWindowResult[T] = {
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

    ApplyUnkeyedWindowResult(windowedStream)
  }

  def applyTimeWindow(windowExpr: TimeWindowExpression,
                      inputKeyedStream: KeyedStream[_, _]): ApplyKeyedWindowResult[_, _, TimeWindow] = {
    val inputTypeName = TypeUtil.getTypeName(inputKeyedStream.getType)
    val keyTypeName = TypeUtil.getTypeName(inputKeyedStream.getKeyType)

    val eval = RuntimeEvaluator.instance

    eval.evalFunction[TimeWindowExpression, KeyedStream[_, _], ApplyKeyedWindowResult[_, _, TimeWindow]](
      "windowExpr",
      eval.getClassName[TimeWindowExpression],
      "inputKeyedStream",
      FlinkTypeNames.keyedStream(inputTypeName, keyTypeName),
      s"${this.typeName}.applyKeyedTimeWindowImpl[$inputTypeName, $keyTypeName](windowExpr, inputKeyedStream)",
      windowExpr,
      inputKeyedStream)
  }

  def applyKeyedTimeWindowImpl[T, TKey](windowExpr: TimeWindowExpression,
                                        inputKeyedStream: KeyedStream[T, TKey]): ApplyKeyedWindowResult[T, TKey, TimeWindow] = {
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

    ApplyKeyedWindowResult(windowedStream, FlinkTypeNames.timeWindow)
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
