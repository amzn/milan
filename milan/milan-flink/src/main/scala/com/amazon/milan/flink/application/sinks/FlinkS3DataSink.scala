package com.amazon.milan.flink.application.sinks

import java.io.OutputStream

import com.amazon.milan.flink.application.{FlinkDataSink, sinks}
import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.core.fs.Path
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, RollingPolicy, StreamingFileSink}


@JsonSerialize
@JsonDeserialize
class FlinkS3DataSink[T](bucket: String, prefix: String, keyFormat: String) extends FlinkDataSink[T] {
  override def getSinkFunction: SinkFunction[T] = {
    val bucketAssigner = new sinks.FlinkS3DataSink.KeyFormatBucketAssigner[T](keyFormat)

    val rollingPolicy = DefaultRollingPolicy.create()
      .withMaxPartSize(1024 * 1024 * 10)
      .withInactivityInterval(5000)
      .withRolloverInterval(5000)
      .build()
      .asInstanceOf[RollingPolicy[T, String]]

    val basePath = s"s3://${this.bucket}/${this.prefix}"

    StreamingFileSink.forRowFormat[T](new Path(basePath), new FlinkS3DataSink.JsonEncoder[T]())
      .withBucketAssigner(bucketAssigner)
      .withRollingPolicy(rollingPolicy)
      .build()
  }

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
  }
}


object FlinkS3DataSink {

  class KeyFormatBucketAssigner[T](keyFormat: String) extends BucketAssigner[T, String] {
    override def getBucketId(in: T, context: BucketAssigner.Context): String = ""

    override def getSerializer: SimpleVersionedSerializer[String] = SimpleVersionedStringSerializer.INSTANCE
  }

  class JsonEncoder[T] extends Encoder[T] {
    @transient private lazy val writer = ScalaObjectMapper.writer()

    override def encode(in: T, outputStream: OutputStream): Unit = {
      this.writer.writeValue(outputStream, in)
      outputStream.write(10)
    }
  }

}
