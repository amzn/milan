package com.amazon.milan.flink.components

import java.util

import com.amazon.milan.flink.RuntimeEvaluator
import com.amazon.milan.flink.compiler.internal._
import com.amazon.milan.flink.types.ArrayRecord
import com.amazon.milan.program.FieldDefinition
import com.amazon.milan.typeutil.FieldDescriptor
import org.apache.commons.lang.builder.HashCodeBuilder
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer


object TupleStreamTypeInformation {
  /**
   * Creates a [[TupleStreamTypeInformation]] for a stream of the specified fields.
   *
   * @param fields A list of (field name, type name) tuples.
   * @return A [[TupleStreamTypeInformation]] for the stream.
   */
  def createFromFieldTypes(fields: List[(String, String)]): TupleStreamTypeInformation = {
    val eval = RuntimeEvaluator.instance

    val fieldTypeInformation =
      fields
        .map {
          case (fieldName, fieldClassName) =>
            FieldTypeInformation(fieldName, eval.createTypeInformation(fieldClassName))
        }
        .toArray

    new TupleStreamTypeInformation(fieldTypeInformation)
  }

  /**
   * Creates a [[TupleStreamTypeInformation]] for a stream of the specified fields.
   *
   * @param fields A list of [[FieldDescriptor]] objects describing the fields.
   * @return A [[TupleStreamTypeInformation]] for the stream.
   */
  def createFromFields(fields: List[FieldDescriptor[_]]): TupleStreamTypeInformation = {
    val fieldsAndTypeNames = fields.map(f => (f.name, f.fieldType.fullName))
    createFromFieldTypes(fieldsAndTypeNames)
  }

  /**
   * Creates a [[TupleStreamTypeInformation]] for a stream of the specified fields.
   *
   * @param fields A list of [[FieldDefinition]] objects describing the fields.
   * @return A [[TupleStreamTypeInformation]] for the stream.
   */
  def createFromFieldDefinitions(fields: List[FieldDefinition]): TupleStreamTypeInformation = {
    val fieldsAndTypeNames = fields.map(f => (f.fieldName, f.expr.tpe.getTypeName))
    createFromFieldTypes(fieldsAndTypeNames)
  }
}


class TupleStreamTypeInformation(val fields: Array[FieldTypeInformation]) extends TypeInformation[ArrayRecord] {
  private val hashCodeValue = HashCodeBuilder.reflectionHashCode(this)

  override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[ArrayRecord] = {
    new TupleStreamTypeSerializer(executionConfig, this.fields)
  }

  override def getGenericParameters: util.Map[String, TypeInformation[_]] = {
    new util.HashMap[String, TypeInformation[_]]()
  }

  override def getArity: Int = this.fields.length

  override def getTotalFields: Int = this.getArity + this.fields.map(_.typeInfo.getTotalFields).sum

  override def getTypeClass: Class[ArrayRecord] = classOf[ArrayRecord]

  override def isBasicType: Boolean = false

  override def isKeyType: Boolean = false

  override def isSortKeyType: Boolean = false

  override def isTupleType: Boolean = true

  override def canEqual(o: Any): Boolean = {
    o match {
      case _: TupleStreamTypeInformation =>
        true

      case _ =>
        false
    }
  }

  override def toString: String = {
    "TupleStream" + this.fields.map(_.typeInfo.toString).mkString("[", ", ", "]")
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: TupleStreamTypeInformation =>
        this.fields.sameElements(o.fields)
    }
  }

  override def hashCode(): Int = this.hashCodeValue
}


case class FieldTypeInformation(fieldName: String, typeInfo: TypeInformation[_]) extends Serializable
