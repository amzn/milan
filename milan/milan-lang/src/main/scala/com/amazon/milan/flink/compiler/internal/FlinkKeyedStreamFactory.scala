package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.flink.components.KeyFunctionKeySelector
import com.amazon.milan.flink.{FlinkTypeNames, RuntimeEvaluator}
import com.amazon.milan.program.{FunctionDef, TypeChecker}
import com.amazon.milan.typeutil.TypeDescriptor
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.datastream.{DataStream, KeyedStream}
import org.slf4j.LoggerFactory


object FlinkKeyedStreamFactory {
  private val logger = Logger(LoggerFactory.getLogger(getClass))

  val typeName: String = getClass.getTypeName.stripSuffix("$")

  /**
   * Converts a [[DataStream]] into a [[KeyedStream]] using a key expression.
   *
   * @param recordType  A [[TypeDescriptor]] describing the type of the records on the stream.
   * @param dataStream  The input data stream.
   * @param keyFunction The function that defines how keys are assigned.
   * @return A [[KeyedStream]].
   */
  def keyStreamByFunction(recordType: TypeDescriptor[_],
                          dataStream: DataStream[_],
                          keyFunction: FunctionDef): KeyedStream[_, _] = {
    TypeChecker.typeCheck(keyFunction, List(recordType))

    val keyType = keyFunction.tpe

    val recordTypeName = recordType.fullName
    val keyTypeName = keyType.fullName
    val eval = RuntimeEvaluator.instance

    eval.evalFunction[TypeDescriptor[_], TypeDescriptor[_], DataStream[_], FunctionDef, KeyedStream[_, _]](
      "recordType",
      TypeDescriptor.typeName(recordTypeName),
      "keyType",
      TypeDescriptor.typeName(keyTypeName),
      "dataStream",
      FlinkTypeNames.dataStream(recordTypeName),
      "keyFunction",
      eval.getClassName[FunctionDef],
      s"${this.typeName}.keyStreamByFunction[$recordTypeName, $keyTypeName](recordType, keyType, dataStream, keyFunction)",
      recordType,
      keyType,
      dataStream,
      keyFunction)
  }

  /**
   * Converts a [[DataStream]] into a [[KeyedStream]] using a key expression.
   *
   * @param recordType  A [[TypeDescriptor]] describing the type of the records on the stream.
   * @param keyType     A [[TypeDescriptor]] describing the key type.
   * @param dataStream  The input data stream.
   * @param keyFunction The function that defines how keys are assigned.
   * @return A [[KeyedStream]].
   */
  def keyStreamByFunction[T, TKey](recordType: TypeDescriptor[T],
                                   keyType: TypeDescriptor[TKey],
                                   dataStream: DataStream[T],
                                   keyFunction: FunctionDef): KeyedStream[T, TKey] = {
    this.logger.info(s"Creating keyed stream using key function: $keyFunction")
    val eval = RuntimeEvaluator.instance

    val keySelector = new KeyFunctionKeySelector[T, TKey](recordType, keyFunction)
    val keyTypeInfo = eval.createTypeInformation(keyType)
    dataStream.keyBy(keySelector, keyTypeInfo)
  }
}
