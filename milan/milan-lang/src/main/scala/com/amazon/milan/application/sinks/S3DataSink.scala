package com.amazon.milan.application.sinks

import com.amazon.milan.application.DataSink
import com.amazon.milan.dataformats.DataOutputFormat
import com.amazon.milan.program.FunctionDef
import com.amazon.milan.program.internal.ConvertExpressionHost
import com.amazon.milan.typeutil.{TypeDescriptor, TypeDescriptorMacroHost}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

import scala.language.experimental.macros
import scala.reflect.macros.whitebox


object S3DataSink {
  /**
   * Creates an [[S3DataSink]].
   *
   * @param bucket             The bucket to which data will be written.
   * @param prefix             The prefix for object keys.
   * @param partitionExtractor A function that returns a tuple that contains the partition keys.
   * @param dataFormat         The output data format.
   * @tparam T              The record type of the stream.
   * @tparam TPartitionKeys The type of the partition key tuple.
   * @return An [[S3DataSink]].
   */
  def create[T, TPartitionKeys <: Product](bucket: String,
                                           prefix: String,
                                           partitionExtractor: T => TPartitionKeys,
                                           dataFormat: DataOutputFormat[T]): S3DataSink[T] = macro S3DataSinkMacros.create[T, TPartitionKeys]
}


/**
 * A data sink that writes items to an S3 bucket.
 *
 * @param bucket             The name of the S3 bucket.
 * @param prefix             The prefix to use for object keys.
 * @param partitionExtractor A [[FunctionDef]] defining the function that assigns partitions to items being written.
 * @param dataFormat         A [[DataOutputFormat]] that controls how items are written.
 * @tparam T The type of objects accepted by the data sink.
 */
@JsonSerialize
@JsonDeserialize
class S3DataSink[T: TypeDescriptor](val sinkId: String,
                                    val bucket: String,
                                    val prefix: String,
                                    val partitionExtractor: FunctionDef,
                                    val dataFormat: DataOutputFormat[T]) extends DataSink[T] {
  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]

  override def getGenericArguments: List[TypeDescriptor[_]] = List(this.recordTypeDescriptor)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }
}


class S3DataSinkMacros(val c: whitebox.Context)
  extends ConvertExpressionHost
    with TypeDescriptorMacroHost {

  import c.universe._

  /**
   * Creates an [[S3DataSink]] using the specified partition extractor function.
   *
   * @param bucket             The name of the S3 bucket.
   * @param prefix             The prefix to use for object keys.
   * @param partitionExtractor The function that assigns partitions to items being written.
   * @param dataFormat         A [[DataOutputFormat]] that controls how items are written.
   * @tparam T              The type of objects accepted by the data sink.
   * @tparam TPartitionKeys The type of the partition key objects.
   * @return
   */
  def create[T: c.WeakTypeTag, TPartitionKeys: c.WeakTypeTag](bucket: c.Expr[String],
                                                              prefix: c.Expr[String],
                                                              partitionExtractor: c.Expr[T => TPartitionKeys],
                                                              dataFormat: c.Expr[DataOutputFormat[T]]): c.Expr[S3DataSink[T]] = {
    val partitionKeysTypeInfo = this.createTypeInfo[TPartitionKeys]
    if (!this.isTuple(partitionKeysTypeInfo.ty)) {
      c.error(c.enclosingPosition, "Partition key must be a tuple.")
    }

    val recordTypeInfo = this.createTypeInfo[T]

    val partitionFunctionDef = this.getMilanFunction(partitionExtractor.tree)
    val tree = q"new ${weakTypeOf[S3DataSink[T]]}($bucket, $prefix, $partitionFunctionDef, $dataFormat)(${recordTypeInfo.toTypeDescriptor})"

    c.Expr[S3DataSink[T]](tree)
  }
}
