package com.amazon.milan.flink.components

import java.lang
import java.time.Instant

import com.amazon.milan.Id
import com.amazon.milan.flink.compiler.internal.{RuntimeCompiledFunction2, SerializableFunction2}
import com.amazon.milan.flink.types.ArrayRecord
import com.amazon.milan.program.FunctionDef
import com.amazon.milan.typeutil.{TypeDescriptor, types}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.windowing.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector


class TimeWindowProcessAllWindowFunction[TIn, TOut](outputMapFunction: SerializableFunction2[Instant, TIn, TOut])
  extends ProcessAllWindowFunction[TIn, TOut, TimeWindow] {

  def this(inputType: TypeDescriptor[_], outputMapFunctionDef: FunctionDef) {
    this(new RuntimeCompiledFunction2[Instant, TIn, TOut](types.Instant, inputType, outputMapFunctionDef))
  }

  override def process(context: ProcessAllWindowFunction[TIn, TOut, TimeWindow]#Context,
                       inputs: lang.Iterable[TIn],
                       collector: Collector[TOut]): Unit = {
    val windowStart = Instant.ofEpochMilli(context.window().getStart)
    val latestInput = inputs.iterator().next()
    val output = this.outputMapFunction(windowStart, latestInput)
    collector.collect(output)
  }
}


class TimeWindowToFieldsProcessWindowFunction[TIn <: Tuple](fieldMapFunctions: List[SerializableFunction2[Instant, Any, Any]])
  extends ProcessAllWindowFunction[TIn, ArrayRecord, TimeWindow] {

  def this(fieldInputTypeNames: List[String], fieldMapFunctionDefs: List[FunctionDef]) {
    this(TimeWindowToFieldsProcessWindowFunction.getFieldMapFunctions(fieldInputTypeNames, fieldMapFunctionDefs))
  }

  override def process(context: ProcessAllWindowFunction[TIn, ArrayRecord, TimeWindow]#Context,
                       inputs: lang.Iterable[TIn],
                       collector: Collector[ArrayRecord]): Unit = {
    val latestInput = inputs.iterator().next()
    val inputValues = Array.tabulate(this.fieldMapFunctions.length)(latestInput.getField[Any])
    val windowStartTime = Instant.ofEpochMilli(context.window().getStart)

    val outputValues =
      inputValues
        .zip(this.fieldMapFunctions)
        .map { case (v, f) => f(windowStartTime, v) }
    val output = new ArrayRecord(Id.newId(), outputValues)
    collector.collect(output)
  }
}

object TimeWindowToFieldsProcessWindowFunction {
  def getFieldMapFunctions(fieldInputTypeNames: List[String],
                           fieldMapFunctionDefs: List[FunctionDef]): List[SerializableFunction2[Instant, Any, Any]] = {
    fieldInputTypeNames.zip(fieldMapFunctionDefs)
      .map {
        case (inputTypeName, functionDef) =>
          val inputStreamNode = TypeDescriptor.forTypeName[Any](inputTypeName)
          new RuntimeCompiledFunction2[Instant, Any, Any](types.Instant, inputStreamNode, functionDef)
      }
  }
}


class TimeWindowGroupByProcessWindowFunction[TIn, TKey, TOut](outputMapFunction: SerializableFunction2[Instant, TIn, TOut])
  extends ProcessWindowFunction[TIn, TOut, TKey, TimeWindow] {

  def this(inputType: TypeDescriptor[_], outputMapFunctionDef: FunctionDef) {
    this(new RuntimeCompiledFunction2[Instant, TIn, TOut](types.Instant, inputType, outputMapFunctionDef))
  }

  override def process(key: TKey,
                       context: ProcessWindowFunction[TIn, TOut, TKey, TimeWindow]#Context,
                       inputs: lang.Iterable[TIn],
                       collector: Collector[TOut]): Unit = {
    val latestInput = inputs.iterator().next()
    val windowStartTime = Instant.ofEpochMilli(context.window().getStart)
    val output = this.outputMapFunction(windowStartTime, latestInput)
    collector.collect(output)
  }
}


class GroupByProcessWindowFunction[TIn, TKey, TOut](outputMapFunction: SerializableFunction2[TKey, TIn, TOut])
  extends ProcessWindowFunction[TIn, TOut, TKey, GlobalWindow] {

  def this(keyTypeName: String, inputType: TypeDescriptor[_], outputMapFunctionDef: FunctionDef) {
    this(new RuntimeCompiledFunction2[TKey, TIn, TOut](TypeDescriptor.forTypeName[TKey](keyTypeName), inputType, outputMapFunctionDef))
  }

  override def process(key: TKey,
                       context: ProcessWindowFunction[TIn, TOut, TKey, GlobalWindow]#Context,
                       inputs: lang.Iterable[TIn],
                       collector: Collector[TOut]): Unit = {
    val latestInput = inputs.iterator().next()
    val output = this.outputMapFunction(key, latestInput)
    collector.collect(output)
  }
}


class GroupByToFieldsProcessWindowFunction[TIn <: Tuple, TKey](fieldMapFunctions: List[SerializableFunction2[TKey, Any, Any]])
  extends ProcessWindowFunction[TIn, ArrayRecord, TKey, GlobalWindow] {

  def this(keyTypeName: String, fieldInputTypeNames: List[String], fieldMapFunctionDefs: List[FunctionDef]) {
    this(GroupByToFieldsProcessWindowFunction.getFieldMapFunctions[TKey](keyTypeName, fieldInputTypeNames, fieldMapFunctionDefs))
  }

  override def process(key: TKey,
                       context: ProcessWindowFunction[TIn, ArrayRecord, TKey, GlobalWindow]#Context,
                       inputs: lang.Iterable[TIn],
                       collector: Collector[ArrayRecord]): Unit = {
    val latestInput = inputs.iterator().next()
    val inputValues = Array.tabulate(this.fieldMapFunctions.length)(latestInput.getField[Any])
    val outputValues =
      inputValues
        .zip(this.fieldMapFunctions)
        .map { case (v, f) => f(key, v) }
    val output = new ArrayRecord(Id.newId(), outputValues)
    collector.collect(output)
  }
}


class TimeWindowGroupByToFieldsProcessWindowFunction[TIn <: Tuple, TKey](fieldMapFunctions: List[SerializableFunction2[Instant, Any, Any]])
  extends ProcessWindowFunction[TIn, ArrayRecord, TKey, TimeWindow] {

  def this(fieldInputTypeNames: List[String], fieldMapFunctionDefs: List[FunctionDef]) {
    this(TimeWindowToFieldsProcessWindowFunction.getFieldMapFunctions(fieldInputTypeNames, fieldMapFunctionDefs))
  }

  override def process(key: TKey,
                       context: ProcessWindowFunction[TIn, ArrayRecord, TKey, TimeWindow]#Context,
                       inputs: lang.Iterable[TIn],
                       collector: Collector[ArrayRecord]): Unit = {
    val latestInput = inputs.iterator().next()
    val inputValues = Array.tabulate(this.fieldMapFunctions.length)(latestInput.getField[Any])
    val windowStartTime = Instant.ofEpochMilli(context.window().getStart)
    val outputValues =
      inputValues
        .zip(this.fieldMapFunctions)
        .map { case (v, f) => f(windowStartTime, v) }
    val output = new ArrayRecord(Id.newId(), outputValues)
    collector.collect(output)
  }
}

object GroupByToFieldsProcessWindowFunction {
  def getFieldMapFunctions[TKey](keyTypeName: String,
                                 fieldInputTypeNames: List[String],
                                 fieldMapFunctionDefs: List[FunctionDef]): List[SerializableFunction2[TKey, Any, Any]] = {
    val keyStreamNode = TypeDescriptor.forTypeName[TKey](keyTypeName)

    fieldInputTypeNames.zip(fieldMapFunctionDefs)
      .map {
        case (inputTypeName, functionDef) =>
          val inputStreamNode = TypeDescriptor.forTypeName[Any](inputTypeName)
          new RuntimeCompiledFunction2[TKey, Any, Any](keyStreamNode, inputStreamNode, functionDef)
      }
  }
}
