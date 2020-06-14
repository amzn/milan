package com.amazon.milan.compiler.flink.runtime

import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer

abstract class PartitionFunctionBucketAssigner[T] extends BucketAssigner[T, String] {
  protected def getPartitionKeys(value: T): Array[String]

  override def getBucketId(value: T, context: BucketAssigner.Context): String = {
    this.getPartitionKeys(value).mkString("/")
  }

  override def getSerializer: SimpleVersionedSerializer[String] = SimpleVersionedStringSerializer.INSTANCE
}
