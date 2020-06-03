package com.amazon.milan.flink.types

import java.util

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

import scala.collection.JavaConverters._


/**
 * A Flink [[TypeInformation]] that provides generic type parameters for parameterized types.
 *
 * @param innerTypeInfo  A [[TypeInformation]] for the type class.
 * @param typeParameters The values of the type parameters.
 * @tparam T The being described.
 */
class ParameterizedTypeInfo[T](innerTypeInfo: TypeInformation[T],
                               typeParameters: List[TypeInformation[_]]) extends TypeInformation[T] {

  override def getGenericParameters: util.Map[String, TypeInformation[_]] = {
    val genericArgumentNames = this.innerTypeInfo.getTypeClass.getTypeParameters.map(_.getName)
    genericArgumentNames.zip(this.typeParameters).toMap.asJava
  }

  override def isBasicType: Boolean = this.innerTypeInfo.isBasicType

  override def isTupleType: Boolean = this.innerTypeInfo.isTupleType

  override def getArity: Int = this.innerTypeInfo.getArity

  override def getTotalFields: Int = this.innerTypeInfo.getTotalFields

  override def getTypeClass: Class[T] = this.innerTypeInfo.getTypeClass

  override def isKeyType: Boolean = this.innerTypeInfo.isKeyType

  override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[T] =
    this.innerTypeInfo.createSerializer(executionConfig)

  override def canEqual(o: Any): Boolean = this.innerTypeInfo.canEqual(o)

  override def toString: String = this.innerTypeInfo.toString

  override def hashCode(): Int = this.innerTypeInfo.hashCode()

  override def equals(obj: Any): Boolean = this.innerTypeInfo.equals(obj)
}
