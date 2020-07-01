package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.application.DataSink
import com.amazon.milan.application.sinks.{ConsoleDataSink, FileDataSink, LogSink, S3DataSink}
import com.amazon.milan.compiler.scala._
import com.amazon.milan.compiler.flink.internal.FlinkScalarFunctionGenerator
import com.amazon.milan.compiler.flink.runtime._
import com.amazon.milan.compiler.flink.types.ArrayRecord
import com.amazon.milan.compiler.flink.typeutil._
import com.amazon.milan.program.{FunctionDef, TypeChecker}
import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.functions.sink.filesystem.{RollingPolicy, StreamingFileSink}


trait DataSinkGenerator {
  val typeLifter: FlinkTypeLifter

  import typeLifter._

  def addDataSink(out: GeneratorOutputs,
                  stream: GeneratedDataStream,
                  sink: DataSink[_]): Unit = {
    val actualStream = this.ifTupleStreamThenMapArrayRecordsToTuples(out, stream)

    sink match {
      case s: S3DataSink[_] =>
        this.addS3DataSink(out, actualStream, s)

      case s: FileDataSink[_] =>
        this.addFileDataSink(out, actualStream, s)

      case _: ConsoleDataSink[_] =>
        this.addPrintSink(out, actualStream)

      case _: LogSink[_] =>
        this.addLogSink(out, actualStream)
    }
  }

  private def ifTupleStreamThenMapArrayRecordsToTuples(out: GeneratorOutputs,
                                                       stream: GeneratedDataStream): GeneratedDataStream = {
    // If it's a stream of named tuples then the record objects will be ArrayRecords, but the data sink doesn't want
    // ArrayRecords it wants the tuples, so we need to map it first.
    if (!stream.recordType.isTupleRecord) {
      stream
    }
    else {
      val mapFunctionClassName = this.generateArrayRecordToTupleMapFunction(out, stream.streamId, stream.recordType, stream.keyType)

      val tupleStreamVal = out.newValName(s"stream_${stream.streamId}_tuples_")
      val mapFunctionVal = out.newValName(s"stream_${stream.streamId}_tupleExtractorMapFunction_")

      val codeBlock =
        q"""
           |val $mapFunctionVal = new $mapFunctionClassName
           |val $tupleStreamVal = ${stream.streamVal}.map($mapFunctionVal)
           |""".codeStrip

      out.appendMain(codeBlock)

      GeneratedUnkeyedDataStream(stream.streamId + "_tuples", tupleStreamVal, stream.recordType.toMilanRecordType, stream.keyType, isContextual = false)
    }
  }

  private def addS3DataSink(out: GeneratorOutputs,
                            stream: GeneratedDataStream,
                            sink: S3DataSink[_]): Unit = {
    val recordType = stream.recordType

    val encoderVal = out.newValName(s"${stream.streamVal}_encoder_")
    val rollingPolicyVal = out.newValName(s"${stream.streamVal}_rollingPolicy_")
    val bucketAssignerVal = out.newValName(s"${stream.streamVal}_bucketAssigner_")
    val sinkFunctionVal = out.newValName(s"${stream.streamVal}_sinkFunction_")
    val outputFormatVal = out.newValName(s"stream_${stream.streamId}_outputFormat_")

    val bucketAssignerClassName = this.createBucketAssigner(out, stream.streamVal.value, sink.partitionExtractor, recordType)

    val basePath = s"s3://${sink.bucket}/${sink.prefix}"

    val code =
      q"""
         |val $outputFormatVal = ${sink.dataFormat}
         |val $encoderVal = new ${nameOf[DataOutputFormatEncoder[Any]]}[${recordType.toFlinkTerm}]($outputFormatVal)
         |val $rollingPolicyVal = ${nameOf[DefaultRollingPolicy[Any, Any]]}.create()
         |  .withMaxPartSize(1024 * 1024 * 10)
         |  .withInactivityInterval(5000)
         |  .withRolloverInterval(5000)
         |  .build()
         |  .asInstanceOf[${nameOf[RollingPolicy[Any, Any]]}[${recordType.toFlinkTerm}, String]]
         |
         |val $bucketAssignerVal = new $bucketAssignerClassName
         |
         |val $sinkFunctionVal = ${nameOf[StreamingFileSink[Any]]}.forRowFormat[${recordType.toFlinkTerm}](new ${nameOf[Path]}($basePath), $encoderVal)
         |  .withBucketAssigner($bucketAssignerVal)
         |  .withRollingPolicy($rollingPolicyVal)
         |  .build()
         |""".codeStrip

    out.appendMain(code)

    this.unwrapAndAddSink(out, stream, sinkFunctionVal, CodeBlock.EMPTY)
  }

  private def addFileDataSink(out: GeneratorOutputs,
                              stream: GeneratedDataStream,
                              sink: FileDataSink[_]): Unit = {
    val outputFormatVal = out.newValName(s"stream_${stream.streamId}_outputFormat_")
    val sinkFunctionVal = out.newValName(s"stream_${stream.streamId}_sinkFunction_")

    val code =
      q"""
         |val $outputFormatVal = ${sink.outputFormat}
         |val $sinkFunctionVal = new ${nameOf[FileSinkFunction[Any]]}[${stream.recordType.toFlinkTerm}](${sink.path}, $outputFormatVal)
         |""".codeStrip
    out.appendMain(code)

    this.unwrapAndAddSink(out, stream, sinkFunctionVal, qc".setParallelism(1)")
  }

  private def addPrintSink(out: GeneratorOutputs,
                           stream: GeneratedDataStream): Unit = {
    val code = q"${stream.streamVal}.print()"
    out.appendMain(code)
  }

  private def addLogSink(out: GeneratorOutputs,
                         stream: GeneratedDataStream): Unit = {
    val code = q"${stream.streamVal}.print()"
    out.appendMain(code)
  }

  private def unwrapAndAddSink(out: GeneratorOutputs,
                               stream: GeneratedDataStream,
                               sinkFunctionVal: ValName,
                               sinkModifiers: CodeBlock): Unit = {
    val recordType = stream.recordType
    val keyType = stream.keyType

    val unwrappedVal = out.newValName(s"${stream.streamVal}_unwrapped_")
    val mapFunctionVal = out.newValName(s"${stream.streamVal}_unwrapMapFunction_")

    val code =
      q"""val $mapFunctionVal = new ${nameOf[UnwrapRecordsMapFunction[Any, Product]]}[${recordType.toFlinkTerm}, ${keyType.toTerm}](
         |  ${liftTypeDescriptorToTypeInformation(recordType)})
         |val $unwrappedVal = ${stream.streamVal}.map($mapFunctionVal)
         |$unwrappedVal.addSink($sinkFunctionVal)$sinkModifiers
         |""".codeStrip

    out.appendMain(code)
  }

  private def createBucketAssigner(out: GeneratorOutputs,
                                   streamIdentifier: String,
                                   partitionFunctionDef: FunctionDef,
                                   recordType: TypeDescriptor[_]): ClassName = {
    val finalPartitionFunction = partitionFunctionDef.withArgumentTypes(List(recordType))
    TypeChecker.typeCheck(finalPartitionFunction)

    val className = out.newClassName(s"BucketAssigner_$streamIdentifier")

    val extractKeysImpl = FlinkScalarFunctionGenerator.default.getScalaFunctionDef("extractKeys", finalPartitionFunction)

    val classDef =
      q"""
         |class $className extends ${nameOf[PartitionFunctionBucketAssigner[Any]]}[${recordType.toFlinkTerm}] {
         |  override def getPartitionKeys(value: ${recordType.toFlinkTerm}): Array[String] = {
         |    ${code(extractKeysImpl).indentTail(2)}
         |    val keys = extractKeys(value)
         |    keys.productIterator.map(_.toString).toArray
         |  }
         |}
         |""".codeStrip

    out.addClassDef(classDef)

    className
  }

  private def generateArrayRecordToTupleMapFunction(out: GeneratorOutputs,
                                                    streamIdentifier: String,
                                                    recordType: TypeDescriptor[_],
                                                    keyType: TypeDescriptor[_]): ClassName = {
    val className = out.newClassName(s"MapFunction_${streamIdentifier}_TupleExtractor")

    val tupleElementExtractors =
      recordType.fields.zipWithIndex.map { case (field, i) =>
        s"record.productElement($i).asInstanceOf[${field.fieldType.toFlinkTerm}]"
      }.mkString(",\n")

    val tupleRecordType = TypeDescriptor.createTuple(recordType.fields.map(_.fieldType))

    val classDef =
      q"""
         |class $className extends ${nameOf[ArrayRecordToTupleMapFunction[Any, Product]]}[${tupleRecordType.toFlinkTerm}, ${keyType.toTerm}](
         |  ${liftTypeDescriptorToTypeInformation(tupleRecordType)},
         |  ${liftTypeDescriptorToTypeInformation(keyType)}) {
         |
         |  override def getTuple(record: ${nameOf[ArrayRecord]}): ${tupleRecordType.toFlinkTerm} = {
         |    ${tupleRecordType.toFlinkTerm}(
         |      ${code(tupleElementExtractors).indentTail(3)}
         |    )
         |  }
         |}
         |""".codeStrip

    out.addClassDef(classDef)

    className
  }
}
