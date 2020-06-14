package com.amazon.milan.compiler.flink.generator

import java.util.concurrent.TimeUnit

import com.amazon.milan.compiler.scala.{ClassName, CodeBlock, DefaultTypeEmitter, TypeEmitter, TypeLifter}
import com.amazon.milan.compiler.flink.TypeUtil
import com.amazon.milan.compiler.flink.types._
import com.amazon.milan.compiler.flink.typeutil._
import com.amazon.milan.typeutil.{FieldDescriptor, TypeDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * Provides methods for lifting objects from runtime to compile-time.
 * Essentially, converting objects into the Scala code that constructs those objects.
 *
 * @param typeEmitter                   The [[TypeEmitter]] to use for emitting type names.
 * @param preventGenericTypeInformation Specifies whether, when converting a TypeDescriptor to a Flink TypeInformation,
 *                                      it should cause a runtime error if the TypeInformation created is a GenericTypeInfo.
 */
class FlinkTypeLifter(typeEmitter: TypeEmitter, preventGenericTypeInformation: Boolean)
  extends TypeLifter(typeEmitter) {

  def this(typeEmitter: TypeEmitter) {
    this(typeEmitter, false)
  }

  def this() {
    this(new DefaultTypeEmitter)
  }

  override def withTypeEmitter(typeEmitter: TypeEmitter): FlinkTypeLifter = {
    new FlinkTypeLifter(typeEmitter, this.preventGenericTypeInformation)
  }

  override def lift(o: Any): String = {
    o match {
      case t: Time => q"${nameOf[Time]}.of(${t.getSize}, ${t.getUnit})"
      case t: java.util.concurrent.TimeUnit => q"${nameOf[TimeUnit]}.${code(t.name())}"
      case _ => super.lift(o)
    }
  }

  /**
   * Gets a [[CodeBlock]] that constructs a [[TypeInformation]] for a type described by a [[TypeDescriptor]].
   *
   * @param typeDescriptor A [[TypeDescriptor]].
   * @return A [[CodeBlock]] that constructs a [[TypeInformation]] for the same type.
   */
  def liftTypeDescriptorToTypeInformation(typeDescriptor: TypeDescriptor[_]): CodeBlock = {
    if (typeDescriptor.isTupleRecord) {
      // This is a tuple type with named fields, which means it's a record type for a stream.
      // For this we use TupleStreamTypeInformation.
      val fieldInfos = typeDescriptor.fields.map(field => this.liftFieldDescriptorToFieldTypeInformation(field)).toArray
      qc"new ${nameOf[ArrayRecordTypeInformation]}($fieldInfos)"
    }
    else if (typeDescriptor.typeName == nameOf[RecordWrapper[Any, Product]].value) {
      val valueType = typeDescriptor.genericArguments.head
      val valueTypeInfo = this.liftTypeDescriptorToTypeInformation(valueType)
      val keyType = typeDescriptor.genericArguments.last
      val keyTypeInfo = this.liftTypeDescriptorToTypeInformation(keyType)
      qc"new ${nameOf[RecordWrapperTypeInformation[Any, Product]]}[${classname(valueType)}, ${classname(keyType)}]($valueTypeInfo, $keyTypeInfo)"
    }
    else if (typeDescriptor.isTuple) {
      if (typeDescriptor.genericArguments.isEmpty) {
        // This is an empty tuple, which doesn't really exist in Scala, but we represent as the Product type.
        qc"new ${nameOf[NoneTypeInformation]}"
      }
      else {
        // This is a tuple type, which we want to expose to Flink using Flink's TupleTypeInfo.
        val tupleClassName = ClassName(TypeUtil.getTupleTypeName(typeDescriptor.genericArguments.map(typeEmitter.getTypeFullName)))
        val elementTypeInfos = typeDescriptor.genericArguments.map(this.liftTypeDescriptorToTypeInformation).toArray
        qc"new ${nameOf[ScalaTupleTypeInformation[Product]]}[$tupleClassName]($elementTypeInfos)"
      }
    }
    else {
      val createTypeInfo =
        if (this.isNestedGenericType(typeDescriptor)) {
          // Nested generic types can be confusing to createTypeInformation, so in those cases we'll use
          // TypeInformation.of, which is not as good but at least won't cause a runtime error.
          CodeBlock(s"${nameOf[TypeInformation[Any]]}.of(classOf[${typeDescriptor.getFlinkTypeFullName}])")
        }
        else {
          CodeBlock(s"org.apache.flink.api.scala.createTypeInformation[${typeDescriptor.getFlinkTypeFullName}]")
        }

      val baseTypeInfo =
        if (this.preventGenericTypeInformation) {
          CodeBlock(s"com.amazon.milan.compiler.flink.runtime.RuntimeUtil.preventGenericTypeInformation($createTypeInfo)")
        }
        else {
          createTypeInfo
        }

      // createTypeInformation will not produce a TypeInformation that exposes the generic type parameters.
      // We need to create TypeInformation for the type parameters ourselves, and then create a TypeInformation that
      // exposes them.
      if (typeDescriptor.genericArguments.isEmpty) {
        baseTypeInfo
      }
      else {
        val typeParameters = typeDescriptor.genericArguments.map(this.liftTypeDescriptorToTypeInformation)
        qc"new ${nameOf[ParameterizedTypeInfo[Any]]}[${classname(typeDescriptor)}]($baseTypeInfo, $typeParameters)"
      }
    }
  }

  /**
   * Gets a [[CodeBlock]] that constructs a [[FieldTypeInformation]] for a field described by a [[FieldDescriptor]].
   *
   * @param fieldDescriptor A [[FieldDescriptor]].
   * @return A [[CodeBlock]] that constructs a [[FieldDescriptor]] for the field.
   */
  def liftFieldDescriptorToFieldTypeInformation(fieldDescriptor: FieldDescriptor[_]): CodeBlock = {
    qc"${nameOf[FieldTypeInformation]}(${fieldDescriptor.name}, ${liftTypeDescriptorToTypeInformation(fieldDescriptor.fieldType)})"
  }

  /**
   * Gets whether a type contains more than one level of nested generic arguments.
   *
   * @param ty A type descriptor.
   * @return True if the type is generic and one of its generic arguments is also generic, otherwise false.
   */
  private def isNestedGenericType(ty: TypeDescriptor[_]): Boolean = {
    ty.genericArguments.exists(_.genericArguments.nonEmpty)
  }
}
