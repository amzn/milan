package com.amazon.milan.flink.generator

import com.amazon.milan.program.FunctionDef
import com.amazon.milan.typeutil.{TypeDescriptor, types}


/**
 * Base trait for all generated streams.
 */
trait GeneratedStream {
  /**
   * The stream identifier.
   */
  val streamId: String

  /**
   * The val that refers to the stream in the Flink program.
   */
  val streamVal: ValName

  /**
   * The type of stream records.
   */
  val recordType: TypeDescriptor[_]

  /**
   * The type of record keys.
   * All streams have a key type, even those that haven't been explicitly keyed.
   * The key type is a tuple, and for streams with no key will be an empty tuple (Unit) type.
   */
  val keyType: TypeDescriptor[_]

  /**
   * Specifies whether this stream forms part of the context of the current expression.
   * If it does, certain properties of the stream, such as record keys, must be maintained by the
   * output of the current expression.
   */
  val isContextual: Boolean

  /**
   * Gets a copy of this [[GeneratedStream]] with the isContextual property set to true.
   */
  def toContextual: GeneratedStream
}


object GeneratedStream {
  def unapply(arg: GeneratedStream): Option[(ValName, TypeDescriptor[_], TypeDescriptor[_], Boolean)] = Some((arg.streamVal, arg.recordType, arg.keyType, arg.isContextual))
}


/**
 * Trait that identifies generated streams that are data streams (not windowed or joined).
 */
trait GeneratedDataStream extends GeneratedStream {
  def withStreamVal(newStreamVal: ValName): GeneratedDataStream =
    this.replace(newStreamVal, this.recordType)

  def withRecordType(newRecordType: TypeDescriptor[_]): GeneratedDataStream =
    this.replace(this.streamVal, newRecordType)

  def replace(newStreamVal: ValName, newRecordType: TypeDescriptor[_]): GeneratedDataStream
}


/**
 * Base trait for generated streams where records are grouped in some way, either keyed or windowed.
 */
trait GeneratedGroupedStream extends GeneratedStream {
  val groupKeyType: TypeDescriptor[_]
}


/**
 * Trait that identifies generated keyed streams.
 */
trait GeneratedKeyedStream extends GeneratedStream

object GeneratedKeyedStream {
  def unapply(arg: GeneratedKeyedStream): Option[(ValName, TypeDescriptor[_], TypeDescriptor[_], Boolean)] =
    Some((arg.streamVal, arg.recordType, arg.keyType, arg.isContextual))
}


/**
 * Trait that identifies generated unkeyed streams.
 */
trait GeneratedUnkeyedStream extends GeneratedStream

object GeneratedUnkeyedStream {
  def unapply(arg: GeneratedUnkeyedStream): Option[(ValName, TypeDescriptor[_], Boolean)] = Some((arg.streamVal, arg.recordType, arg.isContextual))
}


/**
 * Represents a basic generated data stream that is not keyed.
 * Non-keyed streams still can have a key type that is carried with the records, but the stream will not be considered
 * a keyed stream by Flink.
 */
case class GeneratedUnkeyedDataStream(streamId: String,
                                      streamVal: ValName,
                                      recordType: TypeDescriptor[_],
                                      keyType: TypeDescriptor[_],
                                      isContextual: Boolean)
  extends GeneratedStream with GeneratedUnkeyedStream with GeneratedDataStream {

  override def replace(newStreamVal: ValName, newRecordType: TypeDescriptor[_]): GeneratedDataStream =
    GeneratedUnkeyedDataStream(this.streamId, newStreamVal, newRecordType, this.keyType, this.isContextual)

  override def toContextual: GeneratedStream =
    GeneratedUnkeyedDataStream(this.streamId, this.streamVal, this.recordType, this.keyType, isContextual = true)
}


/**
 * Represents a generated keyed data stream.
 */
case class GeneratedKeyedDataStream(streamId: String,
                                    streamVal: ValName,
                                    recordType: TypeDescriptor[_],
                                    keyType: TypeDescriptor[_],
                                    isContextual: Boolean)
  extends GeneratedGroupedStream with GeneratedKeyedStream with GeneratedDataStream {

  override val groupKeyType: TypeDescriptor[_] = this.keyType

  override def replace(newStreamVal: ValName, newRecordType: TypeDescriptor[_]): GeneratedDataStream =
    GeneratedKeyedDataStream(this.streamId, newStreamVal, newRecordType, this.keyType, this.isContextual)

  override def toContextual: GeneratedStream =
    GeneratedKeyedDataStream(this.streamId, this.streamVal, this.recordType, this.keyType, isContextual = true)
}


/**
 * Trait that identifies generated windowed streams.
 */
trait GeneratedWindowedStream extends GeneratedGroupedStream {
  /**
   * The type of the window identifier.
   */
  val windowKeyType: TypeDescriptor[_]
}


/**
 * Represents a generated stream that is keyed and windowed.
 */
case class GeneratedKeyedWindowedStream(streamId: String,
                                        streamVal: ValName,
                                        recordType: TypeDescriptor[_],
                                        keyType: TypeDescriptor[_],
                                        windowKeyType: TypeDescriptor[_],
                                        isContextual: Boolean)
  extends GeneratedWindowedStream with GeneratedKeyedStream {

  override val groupKeyType: TypeDescriptor[_] = this.keyType

  override def toContextual: GeneratedStream =
    GeneratedKeyedWindowedStream(this.streamId, this.streamVal, this.recordType, this.keyType, this.windowKeyType, isContextual = true)
}


/**
 * Represents a generated windowed stream that is not keyed.
 */
case class GeneratedUnkeyedWindowStream(streamId: String,
                                        streamVal: ValName,
                                        recordType: TypeDescriptor[_],
                                        windowKeyType: TypeDescriptor[_],
                                        isContextual: Boolean)
  extends GeneratedWindowedStream with GeneratedUnkeyedStream {

  override val keyType: TypeDescriptor[_] = types.Unit

  override val groupKeyType: TypeDescriptor[_] = this.windowKeyType

  override def toContextual: GeneratedStream =
    GeneratedUnkeyedWindowStream(this.streamId, this.streamVal, this.recordType, this.windowKeyType, isContextual = true)
}


case class GeneratedConnectedStreams(streamId: String,
                                     streamVal: ValName,
                                     unappliedConditions: Option[FunctionDef],
                                     keyType: TypeDescriptor[_],
                                     leftRecordType: TypeDescriptor[_],
                                     rightRecordType: TypeDescriptor[_],
                                     isContextual: Boolean)
  extends GeneratedStream {

  override val recordType: TypeDescriptor[_] =
    TypeDescriptor.createTuple(List(leftRecordType, rightRecordType))

  override def toContextual: GeneratedStream =
    GeneratedConnectedStreams(this.streamId, this.streamVal, this.unappliedConditions, this.keyType, this.leftRecordType, this.rightRecordType, isContextual = true)
}
