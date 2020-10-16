package com.amazon.milan.compiler.scala.event

import com.amazon.milan.compiler.scala._


trait ConsumerGenerator {
  val typeLifter: TypeLifter

  import typeLifter._

  /**
   * Generates a consumer method that consumes records from an input stream, and adds it to the generator output.
   *
   * @param outputs      The generator output collector.
   * @param inputStream  The input stream being consumed.
   * @param consumerInfo Information about the consumer of the input stream records.
   * @param recordArg    The name of the input record argument in the consumer body.
   * @param body         The body of the consumer method.
   */
  def generateConsumer(outputs: GeneratorOutputs,
                       inputStream: StreamInfo,
                       consumerInfo: StreamConsumerInfo,
                       recordArg: ValName,
                       body: CodeBlock): Unit = {
    val consumerName = outputs.getConsumerName(consumerInfo)
    this.generateRecordWrapperMethod(outputs, inputStream, qc"Unit", consumerName.value, recordArg, body)
  }

  /**
   * Generates a method that takes a [[RecordWrapper]] from the specified stream and adds the method to the generator
   * output.
   *
   * @param outputs     The generator output collector.
   * @param inputStream The input stream being consumed.
   * @param methodName  The name of the method to generate.
   * @param recordArg   The name of the input record argument in the consumer body.
   * @param body        The body of the consumer method.
   */
  def generateRecordWrapperMethod(outputs: GeneratorOutputs,
                                  inputStream: StreamInfo,
                                  outputType: CodeBlock,
                                  methodName: String,
                                  recordArg: ValName,
                                  body: CodeBlock): Unit = {
    val args = qc"$recordArg: ${this.getRecordWrapperTypeName(inputStream)}"

    val methodDef =
      q"""private def ${code(methodName)}($args): $outputType = {
         |  ${body.indentTail(1)}
         |}
         |""".codeStrip

    outputs.addMethod(methodDef)
  }

  /**
   * Gets a [[CodeBlock]] containing the full type name of the [[RecordWrapper]] type used for a stream.
   */
  def getRecordWrapperTypeName(streamInfo: StreamInfo): CodeBlock = {
    qc"${nameOf[RecordWrapper[Any, Product]]}[${streamInfo.recordType.toTerm}, ${streamInfo.fullKeyType.toTerm}]"
  }
}
