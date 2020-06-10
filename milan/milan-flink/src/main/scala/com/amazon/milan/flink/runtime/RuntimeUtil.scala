package com.amazon.milan.flink.runtime

import java.util

import com.amazon.milan.flink.generator.FlinkGeneratorException
import com.amazon.milan.serialization.MilanObjectMapper
import com.fasterxml.jackson.databind.`type`.TypeFactory
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}


object RuntimeUtil {
  val typeName: String = getClass.getTypeName.stripSuffix("$")

  def loadJsonList[TElement: ClassTag](listJson: String): List[TElement] = {
    this.loadJsonArrayList[TElement](listJson).asScala.toList
  }

  def loadJsonArrayList[TElement: ClassTag](listJson: String): util.ArrayList[TElement] = {
    val typeFactory = TypeFactory.defaultInstance()
    val itemClass = classTag[TElement].runtimeClass.asInstanceOf[Class[TElement]]
    val javaType = typeFactory.constructCollectionType(classOf[util.ArrayList[TElement]], itemClass)
    MilanObjectMapper.readValue[util.ArrayList[TElement]](listJson, javaType)
  }

  def preventGenericTypeInformation[T](typeInfo: TypeInformation[T]): TypeInformation[T] = {
    if (typeInfo.getClass.getName.contains("__wrapper")) {
      throw new FlinkGeneratorException(s"Creating TypeInformation for '${typeInfo.getTypeClass.getName}' produced a GenericTypeInformation.")
    }

    typeInfo
  }
}
