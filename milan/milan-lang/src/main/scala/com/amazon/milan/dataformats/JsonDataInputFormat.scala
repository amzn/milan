package com.amazon.milan.dataformats

import com.amazon.milan.HashUtil
import com.amazon.milan.serialization.{DataFormatConfiguration, JavaTypeFactory, MilanObjectMapper}
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

import java.io.InputStream
import scala.collection.JavaConverters._
import scala.language.experimental.macros


/**
 * A [[DataInputFormat]] for JSON-encoded objects.
 *
 * @param config Configuration controlling data handling.
 * @tparam T The type of objects.
 */
@JsonSerialize
@JsonDeserialize
class JsonDataInputFormat[T: TypeDescriptor](val config: DataFormatConfiguration)
  extends DataInputFormat[T] {

  @transient private lazy val objectMapper = new MilanObjectMapper(this.config)
  @transient private lazy val javaType = new JavaTypeFactory(this.objectMapper.getTypeFactory).makeJavaType(this.recordTypeDescriptor)
  @transient private lazy val hashCodeValue = HashUtil.combineHashCodes(this.recordTypeDescriptor.hashCode(), this.config.hashCode())

  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]

  def this() {
    this(DataFormatConfiguration.default)
  }

  override def getGenericArguments: List[TypeDescriptor[_]] =
    List(implicitly[TypeDescriptor[T]])

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }

  override def readValue(bytes: Array[Byte], offset: Int, length: Int): Option[T] = {
    Some(this.objectMapper.readValue[T](bytes, offset, length, this.javaType))
  }

  override def readValues(stream: InputStream): TraversableOnce[T] = {
    this.objectMapper.readerFor(this.javaType).readValues[T](stream).asScala
  }

  override def hashCode(): Int = this.hashCodeValue

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: JsonDataInputFormat[T] =>
        this.recordTypeDescriptor.equals(o.recordTypeDescriptor) &&
          this.config.equals(o.config)

      case _ =>
        false
    }
  }
}
