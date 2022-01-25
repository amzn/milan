package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.sinks.DynamoDbTableSink
import com.amazon.milan.aws.serverless.DynamoDbSerializer
import com.amazon.milan.aws.serverless.runtime.DynamoDbRecordWriter
import com.amazon.milan.compiler.scala._
import com.amazon.milan.compiler.scala.event.{GeneratorContext, StreamInfo}
import com.amazon.milan.typeutil.{FieldDescriptor, TypeDescriptor, types}
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import java.util

class DynamoDbItemGenerator(typeLifter: TypeLifter) {

  import typeLifter._

  /**
   * Generates a field in the generated class, which contains the record writer object used to write records for the
   * specified stream to the specified DynamoDb table.
   *
   * @param context The generator context.
   * @param stream  The stream whose records are being sent to the sink.
   * @param sink    The sink definition.
   * @return The name of the generated field.
   */
  def generateDynamoDbRecordWriterField(context: GeneratorContext,
                                        stream: StreamInfo,
                                        sink: DynamoDbTableSink[_]): String = {
    // Add a field to the output that is the serializer.
    val serializerFieldName = context.outputs.newFieldName(s"recordSerializer_${stream.streamId}_")
    val serializerClassDef = this.generateDynamoDbSerializerClassDefinition(stream.recordType)
    val serializerFieldDef = s"private val $serializerFieldName = new $serializerClassDef"
    context.outputs.addField(serializerFieldDef)

    // Add a field to the output that is the writer, which uses the serializer we generated above.
    val writerFieldName = context.outputs.newFieldName(s"recordWriter_${stream.streamId}_")
    val writerInstance = this.generateDynamoDbRecordWriterInstance(sink.tableName, serializerFieldName)
    val writerFieldDef = s"private val $writerFieldName = $writerInstance"
    context.outputs.addField(writerFieldDef)

    writerFieldName
  }

  /**
   * Generates code that adds field values from a record object, converted to DynamoDb AttributeValue instances, to a
   * Map.
   *
   * @param objectVal  The [[ValName]] that refers to the instance of the object whose fields are being added to the map.
   * @param mapVal     The [[ValName]] that refers to the map to which the field values are being added.
   * @param objectType The type of the object.
   * @return A [[CodeBlock]] containing the code that adds the field values to the map.
   */
  def generateAddMapValuesFromObject(objectVal: ValName, mapVal: ValName, objectType: TypeDescriptor[_]): CodeBlock = {
    val setFieldStatements = objectType.fields.map(field => this.generateSetFieldInMap(mapVal, objectVal, field))
    qc"""//$setFieldStatements"""
  }

  private def generateDynamoDbSerializerClassDefinition(recordType: TypeDescriptor[_]): CodeBlock = {
    val recordVal = ValName("record")
    val mapVal = ValName("map")
    val createMapCode = this.generateAddMapValuesFromObject(recordVal, mapVal, recordType)

    qc"""
        |${nameOf[DynamoDbSerializer[Any]]}[${recordType.toTerm}] {
        |  override def getAttributeValueMap($recordVal: ${recordType.toTerm}): ${nameOf[util.HashMap[Any, Any]]}[String, ${nameOf[AttributeValue]}] = {
        |    val $mapVal = new ${nameOf[util.HashMap[Any, Any]]}[String, ${nameOf[AttributeValue]}]()
        |    ${createMapCode.indentTail(2)}
        |    $mapVal
        |  }
        |}
        |"""
  }

  private def generateDynamoDbRecordWriterInstance(tableName: String,
                                                   serializerFieldName: String): CodeBlock = {
    qc"""${nameOf[DynamoDbRecordWriter[Any]]}.open($tableName, ${code(serializerFieldName)})"""
  }

  private def generateSetFieldInMap(mapVal: ValName, objectVal: ValName, field: FieldDescriptor[_]): CodeBlock = {
    val fieldValue = this.generateGetFieldValue(objectVal, field)
    qc"""$mapVal.put(${field.name}, ${fieldValue.indentTail(1)})"""
  }

  private def generateGetFieldValue(objectVal: ValName, field: FieldDescriptor[_]): CodeBlock = {
    val objectField = ValName(s"$objectVal.${field.name}")
    this.generateCreateAttributeValue(objectField, field.fieldType)
  }

  private def generateCreateAttributeValue(value: ValName, valueType: TypeDescriptor[_]): CodeBlock = {
    if (valueType.isNumeric) {
      qc"""com.amazon.milan.aws.serverless.runtime.DynamoDb.n($value.toString).asInstanceOf[${nameOf[AttributeValue]}]"""
    }
    else if (valueType == types.String) {
      qc"""com.amazon.milan.aws.serverless.runtime.DynamoDb.s($value).asInstanceOf[${nameOf[AttributeValue]}]"""
    }
    else if (valueType == types.Boolean) {
      qc"""com.amazon.milan.aws.serverless.runtime.DynamoDb.b($value).asInstanceOf[${nameOf[AttributeValue]}]"""
    }
    else if (valueType.isCollection) {
      val itemVal = ValName("item")

      val itemAttributeValueCode = this.generateCreateAttributeValue(itemVal, valueType.genericArguments.head)

      qc"""
          |{
          |  val items = $value.map($itemVal => $itemAttributeValueCode)
          |  com.amazon.milan.aws.serverless.runtime.DynamoDb.l(items).asInstanceOf[${nameOf[AttributeValue]}]
          |}
          |"""
    }
    else {
      val mapVal = ValName("map")
      val hashMapCode = this.generateAddMapValuesFromObject(value, mapVal, valueType)
      qc"""
          |{
          |  val $mapVal = new ${nameOf[util.HashMap[Any, Any]]}[String, Any]()
          |  ${hashMapCode.indentTail(1)}
          |  com.amazon.milan.aws.serverless.runtime.DynamoDb.m(map).asInstanceOf[${nameOf[AttributeValue]}]
          |}
          |"""
    }
  }
}
