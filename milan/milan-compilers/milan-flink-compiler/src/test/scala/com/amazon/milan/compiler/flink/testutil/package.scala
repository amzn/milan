package com.amazon.milan.compiler.flink

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.amazon.milan.compiler.flink.runtime.{UnwrapRecordsMapFunction, WrapRecordsMapFunction}
import com.amazon.milan.compiler.flink.testing.IntKeyValueRecord
import com.amazon.milan.compiler.flink.types.{RecordWrapper, RecordWrapperTypeInformation}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.core.memory.{DataInputView, DataInputViewStreamWrapper, DataOutputView, DataOutputViewStreamWrapper}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.language.implicitConversions
import scala.util.Random


package object testutil {
  def getTestExecutionEnvironment: StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setBufferTimeout(0)
    env
  }

  def copyWithSerializer[T](value: T, serializer: TypeSerializer[T]): T = {
    val outputStream = new ByteArrayOutputStream()
    val outputView = new DataOutputViewStreamWrapper(outputStream)
    serializer.serialize(value, outputView)

    val bytes = outputStream.toByteArray
    val inputStream = new ByteArrayInputStream(bytes)
    val inputView = new DataInputViewStreamWrapper(inputStream)
    serializer.deserialize(inputView)
  }

  def copyData[T](writeValue: DataOutputView => Unit, readValue: DataInputView => T): T = {
    val outputStream = new ByteArrayOutputStream()
    val outputView = new DataOutputViewStreamWrapper(outputStream)
    writeValue(outputView)

    val bytes = outputStream.toByteArray
    val inputStream = new ByteArrayInputStream(bytes)
    val inputView = new DataInputViewStreamWrapper(inputStream)
    readValue(inputView)
  }

  def generateIntKeyValueRecords(recordCount: Int, keyCount: Int, maxValue: Int): List[IntKeyValueRecord] = {
    val rand = new Random(0)
    List.tabulate(recordCount)(_ => IntKeyValueRecord(rand.nextInt(keyCount), rand.nextInt(maxValue + 1)))
  }

  implicit class WrappedDataStreamExtensions[T >: Null, TKey >: Null <: Product](dataStream: DataStream[RecordWrapper[T, TKey]]) {
    def unwrap(recordTypeInformation: TypeInformation[T]): DataStream[T] = {
      val mapper = new UnwrapRecordsMapFunction[T, TKey](recordTypeInformation)
      this.dataStream.map(mapper)
    }

    def unwrap(): DataStream[T] = {
      val recordType = this.dataStream.getType.asInstanceOf[RecordWrapperTypeInformation[T, TKey]].valueTypeInformation
      this.unwrap(recordType)
    }
  }

  implicit class DataStreamExtensions[T >: Null](dataStream: DataStream[T]) {
    def wrap(recordTypeInformation: TypeInformation[T]): DataStream[RecordWrapper[T, Product]] = {
      val mapper = new WrapRecordsMapFunction[T](recordTypeInformation)
      this.dataStream.map(mapper)
    }

    def wrap(): DataStream[RecordWrapper[T, Product]] = {
      val recordType = this.dataStream.asInstanceOf[ResultTypeQueryable[T]].getProducedType
      this.wrap(recordType)
    }
  }

}
